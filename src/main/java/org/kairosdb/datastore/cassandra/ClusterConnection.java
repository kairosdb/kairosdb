package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 Created by bhawkins on 4/29/17.
 */
public class ClusterConnection
{
	public static final Logger logger = LoggerFactory.getLogger(ClusterConnection.class);



	public enum Type
	{
		WRITE,
		META,
		READ
	}

	public static final String CREATE_KEYSPACE = "" +
			"CREATE KEYSPACE IF NOT EXISTS %s" +
			"  WITH REPLICATION = %s";

	public static final String DATA_POINTS_TABLE_NAME = "data_points";
	public static final String DATA_POINTS_TABLE = "" +
			"CREATE TABLE IF NOT EXISTS "+DATA_POINTS_TABLE_NAME+" (\n" +
			"  key blob,\n" +
			"  column1 blob,\n" +
			"  value blob,\n" +
			"  PRIMARY KEY ((key), column1)\n" +
			")";

	//This is effectively what the above table is
	/*public static final String DATA_POINTS_TABLE = "" +
			"CREATE TABLE IF NOT EXISTS data_points (\n" +
			"  metric text, " +
			"  row_time timestamp, " +
			"  data_type text, " +
			"  tags frozen<map<text, text>>, " +
			"  offset int, "+
			"  value blob, " +
			"  PRIMARY KEY ((metric, row_time, data_type, tags), offset)" +
			")";*/

	public static final String ROW_KEY_INDEX_TABLE_NAME = "row_key_index";
	public static final String ROW_KEY_INDEX_TABLE = "" +
			"CREATE TABLE IF NOT EXISTS "+ROW_KEY_INDEX_TABLE_NAME+" (\n" +
			"  key blob,\n" +
			"  column1 blob,\n" +
			"  value blob,\n" +
			"  PRIMARY KEY ((key), column1)\n" +
			")";

	public static final String ROW_KEY_TIME_INDEX_NAME = "row_key_time_index";
	public static final String ROW_KEY_TIME_INDEX = "" +
			"CREATE TABLE IF NOT EXISTS "+ROW_KEY_TIME_INDEX_NAME+" (\n" +
			"  metric text,\n" +
			"  table_name text,\n" +
			"  row_time timestamp,\n" +
			"  value text,\n" +
			"  PRIMARY KEY ((metric), table_name, row_time)\n" +
			")";

	public static final String ROW_KEYS_NAME = "row_keys";
	public static final String ROW_KEYS = "" +
			"CREATE TABLE IF NOT EXISTS "+ROW_KEYS_NAME+" (\n" +
			"  metric text,\n" +
			"  table_name text, \n" +
			"  row_time timestamp,\n" +
			"  data_type text,\n" +
			"  tags frozen<map<text, text>>,\n" +
			"  mtime timeuuid static,\n" +
			"  value text,\n" +
			"  PRIMARY KEY ((metric, table_name, row_time), data_type, tags)\n" +
			")";

	/**
	 * Alternate form of the row_keys table which includes a hash of each tag key:value pair in the
	 * partion key. This is used to improve lookups for high tag cardinality.
	 */
	public static final String TAG_INDEXED_ROW_KEYS_NAME = "tag_indexed_row_keys";
	public static final String TAG_INDEXED_ROW_KEYS = "" +
			"CREATE TABLE IF NOT EXISTS "+TAG_INDEXED_ROW_KEYS_NAME+" (\n" +
			"  metric text,\n" +
			"  table_name text, \n" +
			"  row_time timestamp,\n" +
			"  single_tag_pair text,\n" +
			"  tag_collection_hash int,\n" +
			"  data_type text,\n" +
			"  tags frozen<map<text, text>>,\n" +
			"  mtime timeuuid static,\n" +
			"  value text,\n" +
			"  PRIMARY KEY ((metric, table_name, row_time, single_tag_pair), data_type, tag_collection_hash, tags)\n" +
			")";

	public static final String STRING_INDEX_TABLE_NAME = "string_index";
	public static final String STRING_INDEX_TABLE = "" +
			"CREATE TABLE IF NOT EXISTS "+STRING_INDEX_TABLE_NAME+" (\n" +
			"  key blob,\n" +
			"  column1 text,\n" +
			"  value blob,\n" +
			"  PRIMARY KEY ((key), column1)\n" +
			")";

	public static final String SERVICE_INDEX_NAME = "service_index";
	public static final String SERVICE_INDEX = "" +
			"CREATE TABLE IF NOT EXISTS "+SERVICE_INDEX_NAME+" (" +
			" service text," +
			" service_key text," +
			" key text," +
			" mtime timeuuid static, "+
			" value text," +
			" PRIMARY KEY ((service, service_key), key)" +
			")";



	//All inserts and deletes add millisecond timestamp consistency with old code and TWCS instead of nanos
	public static final String DATA_POINTS_INSERT = "INSERT INTO data_points " +
			"(key, column1, value) VALUES (?, ?, ?) USING TTL ? AND TIMESTAMP ?";

	public static final String ROW_KEY_TIME_INSERT = "INSERT INTO row_key_time_index " +
			"(metric, table_name, row_time) VALUES (?, ?, ?) USING TTL ?";

	public static final String ROW_KEY_INSERT = "INSERT INTO row_keys " +
			"(metric, table_name, row_time, data_type, tags, mtime) VALUES (?, ?, ?, ?, ?, now()) USING TTL ?"; // AND TIMESTAMP ?";

	public static final String TAG_INDEXED_ROW_KEY_INSERT = "INSERT INTO tag_indexed_row_keys " +
			"(metric, table_name, row_time, data_type, single_tag_pair, tag_collection_hash, tags, mtime) VALUES (?, ?, ?, ?, ?, ?, ?, now()) USING TTL ?";

	public static final String STRING_INDEX_INSERT = "INSERT INTO string_index " +
			"(key, column1, value) VALUES (?, ?, 0x00)";

	public static final String STRING_INDEX_QUERY = "SELECT column1 FROM string_index " +
			"WHERE key = ?";

	public static final String STRING_INDEX_PREFIX_QUERY = "SELECT column1 FROM string_index " +
			"WHERE key = ? and column1 >= ? and column1 < ?";

	public static final String STRING_INDEX_DELETE = "DELETE FROM string_index " +
			"WHERE key = ? AND column1 = ?";

	public static final String DATA_POINTS_QUERY = "SELECT column1, value FROM data_points WHERE key = ? AND " +
			"column1 >= ? AND column1 < ? ORDER BY column1";

	public static final String DATA_POINTS_QUERY_ASC = DATA_POINTS_QUERY+" ASC";
	public static final String DATA_POINTS_QUERY_DESC = DATA_POINTS_QUERY+" DESC";

	public static final String DATA_POINTS_QUERY_ASC_LIMIT = DATA_POINTS_QUERY_ASC+" LIMIT ?";
	public static final String DATA_POINTS_QUERY_DESC_LIMIT = DATA_POINTS_QUERY_DESC+" LIMIT ?";

	public static final String DATA_POINTS_DELETE_RANGE = "DELETE FROM data_points " +
			"WHERE key = ? AND column1 >= ? AND column1 <= ?";

	public static final String DATA_POINTS_DELETE = "DELETE FROM data_points " +
			"WHERE key = ? AND column1 = ?";

	public static final String DATA_POINTS_DELETE_ROW = "DELETE FROM data_points " +
			"WHERE key = ?";



	//This is the old row key index query
	public static final String ROW_KEY_INDEX_QUERY = "SELECT column1 FROM row_key_index " +
			"WHERE key = ? AND column1 >= ? AND column1 < ?";

	public static final String ROW_KEY_INDEX_DELETE = "DELETE FROM row_key_index " +
			"WHERE KEY = ? AND column1 = ?";

	public static final String ROW_KEY_INDEX_DELETE_ROW = "DELETE FROM row_key_index " +
			"WHERE KEY = ?";

	//New Row key queries
	public static final String ROW_KEY_TIME_QUERY = "SELECT row_time " +
			"FROM row_key_time_index WHERE metric = ? AND table_name = ? AND " +
			"row_time >= ? AND row_time <= ?";

	public static final String ROW_KEY_QUERY = "SELECT row_time, data_type, tags " +
			"FROM row_keys WHERE metric = ? AND table_name = ? AND row_time = ?";

	public static final String TAG_INDEXED_ROW_KEY_QUERY = "SELECT row_time, data_type, tags, tag_collection_hash " +
			"FROM tag_indexed_row_keys WHERE metric = ? AND table_name = ? AND row_time = ? and single_tag_pair = ? " +
			"ORDER BY data_type, tag_collection_hash";

	public static final String ROW_KEY_TAG_QUERY_WITH_TYPE = "SELECT row_time, data_type, tags " +
			"FROM row_keys WHERE metric = ? AND table_name = 'data_points' AND row_time = ? AND data_type IN %s"; //Use ValueSequence when setting this

	public static final String ROW_KEY_TIME_DELETE = "DELETE FROM row_key_time_index " +
			"WHERE metric = ? AND table_name = ? AND row_time = ?";

	public static final String ROW_KEY_DELETE = "DELETE FROM row_keys WHERE metric = ? " +
			"AND table_name = ? AND row_time = ? AND data_type = ? " +
			"AND tags = ?";

	public static final String TAG_INDEXED_ROW_KEY_DELETE = "DELETE FROM tag_indexed_row_keys WHERE metric = ? " +
			"AND table_name = ? AND row_time = ? AND data_type = ? " +
			"AND single_tag_pair = ? AND tag_collection_hash = ? AND tags = ?";

	//Service index queries
	public static final String SERVICE_INDEX_INSERT = "INSERT INTO service_index " +
			"(service, service_key, key, value, mtime) VALUES (?, ?, ?, ?, now())";

	public static final String SERVICE_INDEX_GET = "SELECT value, WRITETIME(value) " +
			"FROM service_index WHERE service = ? AND service_key = ? AND key = ?";

	public static final String SERVICE_INDEX_LIST_KEYS = "SELECT key " +
			"FROM service_index WHERE service = ? AND service_key = ? ORDER BY key ASC";

	public static final String SERVICE_INDEX_LIST_KEYS_PREFIX = "SELECT key " +
			"FROM service_index WHERE service = ? AND service_key = ? AND " +
			"key >= ? AND key < ?";

	public static final String SERVICE_INDEX_LIST_SERVICE_KEYS = "SELECT service_key " +
			"FROM service_index WHERE service = ? ALLOW FILTERING";

	public static final String SERVICE_INDEX_DELETE_KEY = "DELETE FROM service_index " +
			"WHERE service = ? AND service_key = ? AND key = ?";

	public static final String SERVICE_INDEX_LAST_MODIFIED_TIME = "select mtime from service_index " +
			"WHERE service = ? AND service_key = ? LIMIT 1";

	public static final String SERVICE_INDEX_GET_ENTRIES = "select key, value from service_index " +
			"WHERE service = ? AND service_key = ?";

	public static final String SERVICE_INDEX_INSERT_MODIFIED_TIME = "INSERT INTO service_index " +
			"(service, service_key, mtime) VALUES (?, ?, now())";

	public PreparedStatement psDataPointsInsert;
	//public final PreparedStatement m_psInsertRowKey;
	public PreparedStatement psStringIndexInsert;
	public PreparedStatement psDataPointsQueryAsc;
	public PreparedStatement psStringIndexQuery;
	public PreparedStatement psStringIndexPrefixQuery;
	public PreparedStatement psStringIndexDelete;
	public PreparedStatement psRowKeyIndexQuery;
	public PreparedStatement psRowKeyQuery;
	public PreparedStatement psTagIndexedRowKeyQuery;
	public PreparedStatement psRowKeyTimeQuery;
	public PreparedStatement psDataPointsDeleteRow;
	public PreparedStatement psDataPointsDeleteRange;
	public PreparedStatement psRowKeyIndexDelete;
	public PreparedStatement psRowKeyIndexDeleteRow;
	public PreparedStatement psDataPointsQueryDesc;
	public PreparedStatement psRowKeyTimeInsert;
	public PreparedStatement psRowKeyInsert;
	public PreparedStatement psTagIndexedRowKeyInsert;
	public PreparedStatement psDataPointsQueryAscLimit;
	public PreparedStatement psDataPointsQueryDescLimit;
	public PreparedStatement psServiceIndexInsert;
	public PreparedStatement psServiceIndexGet;
	public PreparedStatement psServiceIndexListKeys;
	public PreparedStatement psServiceIndexListKeysPrefix;
	public PreparedStatement psServiceIndexListServiceKeys;
	public PreparedStatement psServiceIndexDeleteKey;
	public PreparedStatement psRowKeyTimeDelete;
	public PreparedStatement psRowKeyDelete;
	public PreparedStatement psTagIndexedRowKeyDelete;
	public PreparedStatement psServiceIndexModificationTime;
	public PreparedStatement psServiceIndexInsertModifiedTime;
	public PreparedStatement psServiceIndexGetEntries;
	public PreparedStatement psDataPointsDelete;

	private Session m_session;
	private final CassandraClient m_cassandraClient;
	private boolean m_readonlyMode;
	private final EnumSet<Type> m_clusterType;
	private volatile boolean m_shuttingDown = false;

	private final boolean m_alwaysUseTagIndexedLookup;
	private final Multimap<String, String> m_tagIndexMetricNames;

	private final RowKeysTableLookup m_indexedRowKeyLookup = new TagIndexedRowKeysTableLookup();
	private final RowKeysTableLookup m_rowKeyLookup = new RowKeysTableLookup();

	private final CassandraConfiguration m_cassandraConfiguration;


	public ClusterConnection(CassandraConfiguration cassandraConfig, CassandraClient cassandraClient, EnumSet<Type> clusterType,
			Multimap<String, String> tagIndexMetricNames)
	{
		m_cassandraConfiguration = cassandraConfig;
		m_cassandraClient = cassandraClient;
		m_clusterType = clusterType;

		m_alwaysUseTagIndexedLookup = tagIndexMetricNames.containsKey("\"*\"");
		m_tagIndexMetricNames = tagIndexMetricNames;

		if (m_alwaysUseTagIndexedLookup)
		{
			logger.info("Using tag-indexed row key lookup for all metrics for cluster {}",
					cassandraClient.getClusterConfiguration().getClusterName());
		}
		else if (m_tagIndexMetricNames.isEmpty())
		{
			logger.info("Indexed tag-indexed row key lookup is disabled for cluster {}",
					cassandraClient.getClusterConfiguration().getClusterName());
		}
		else
		{
			logger.info("Using tag-indexed row key lookup for {} for cluster {}", m_tagIndexMetricNames,
					cassandraClient.getClusterConfiguration().getClusterName());
		}
	}

	/**
	 Startup the client connection to cassandra and try to set schema
	 @param async if set to true the connection is established in a background
	 thread that continues to connect if C* is not available
	 */
	public ClusterConnection startup(boolean async)
	{
		if (async)
		{
			new Thread(() -> {
				boolean connected = false;
				while (!connected && !m_shuttingDown)
				{
					try
					{
						tryToConnect();
						connected = true;
					}
					catch (Exception e)
					{
						logger.error("Unable to connect to Cassandra", e);
						m_cassandraClient.close();
						m_cassandraClient.init();
					}

					try
					{
						Thread.sleep(1000);
					}
					catch (InterruptedException e)
					{
						//nothing to do here
					}
				}
			}).start();
		}
		else
		{
			tryToConnect();
		}

		return this;
	}

	private void tryToConnect()
	{
		setupSchema(m_cassandraClient, m_clusterType);

		m_session = m_cassandraClient.getKeyspaceSession();

		//m_psInsertRowKey      = m_session.prepare(ROW_KEY_INDEX_INSERT);

		if (m_clusterType.contains(Type.READ) || m_clusterType.contains(Type.WRITE))
		{
			psDataPointsInsert = m_session.prepare(DATA_POINTS_INSERT);
			psDataPointsDelete = m_session.prepare(DATA_POINTS_DELETE);
			psDataPointsDeleteRow = m_session.prepare(DATA_POINTS_DELETE_ROW);
			try
			{
				psDataPointsDeleteRange = m_session.prepare(DATA_POINTS_DELETE_RANGE);
			}
			catch (Exception e)
			{
				//Nothing to do, we run old format delete if psDataPointsDeleteRange is null
				logger.warn("Unable to perform efficient range deletes, consider upgrading to a newer version of Cassandra");
			}
			psDataPointsQueryAsc = m_session.prepare(DATA_POINTS_QUERY_ASC);
			psDataPointsQueryDesc = m_session.prepare(DATA_POINTS_QUERY_DESC);
			psDataPointsQueryAscLimit = m_session.prepare(DATA_POINTS_QUERY_ASC_LIMIT);
			psDataPointsQueryDescLimit = m_session.prepare(DATA_POINTS_QUERY_DESC_LIMIT);

			psRowKeyIndexQuery = m_session.prepare(ROW_KEY_INDEX_QUERY);
			psRowKeyIndexDelete = m_session.prepare(ROW_KEY_INDEX_DELETE);
			psRowKeyIndexDeleteRow = m_session.prepare(ROW_KEY_INDEX_DELETE_ROW);
			try
			{
				psRowKeyQuery = m_session.prepare(ROW_KEY_QUERY);
				psTagIndexedRowKeyQuery = m_session.prepare(TAG_INDEXED_ROW_KEY_QUERY);
				psRowKeyTimeQuery = m_session.prepare(ROW_KEY_TIME_QUERY);
			}
			catch (InvalidQueryException e)
			{
				// Reading data from an older version of Kairos. This table did not exist so ignore.
				if (!e.getMessage().startsWith("unconfigured columnfamily row_key"))
				{
					throw e;
				}
			}

			psStringIndexInsert = m_session.prepare(STRING_INDEX_INSERT);
			psStringIndexQuery = m_session.prepare(STRING_INDEX_QUERY);
			psStringIndexPrefixQuery = m_session.prepare(STRING_INDEX_PREFIX_QUERY);
			psStringIndexDelete = m_session.prepare(STRING_INDEX_DELETE);
		}


		if ((!m_readonlyMode)&&(m_clusterType.contains(Type.WRITE)))
		{
			psRowKeyInsert = m_session.prepare(ROW_KEY_INSERT);
			psTagIndexedRowKeyInsert = m_session.prepare(TAG_INDEXED_ROW_KEY_INSERT);
			psRowKeyDelete = m_session.prepare(ROW_KEY_DELETE);
			psTagIndexedRowKeyDelete = m_session.prepare(TAG_INDEXED_ROW_KEY_DELETE);
			psRowKeyTimeDelete = m_session.prepare(ROW_KEY_TIME_DELETE);
			psRowKeyTimeInsert = m_session.prepare(ROW_KEY_TIME_INSERT);
		}

		if (m_clusterType.contains(Type.META))
		{
			psServiceIndexInsert = m_session.prepare(SERVICE_INDEX_INSERT);
			psServiceIndexGet = m_session.prepare(SERVICE_INDEX_GET);
			psServiceIndexListKeys = m_session.prepare(SERVICE_INDEX_LIST_KEYS);
			psServiceIndexListKeysPrefix = m_session.prepare(SERVICE_INDEX_LIST_KEYS_PREFIX);
			try
			{
				psServiceIndexListServiceKeys = m_session.prepare(SERVICE_INDEX_LIST_SERVICE_KEYS);
			}
			catch (Exception e)
			{
				logger.warn("Unable to perform service key list query, consider upgrading to newer version of Cassandra");
			}
			psServiceIndexDeleteKey = m_session.prepare(SERVICE_INDEX_DELETE_KEY);
			psServiceIndexModificationTime = m_session.prepare(SERVICE_INDEX_LAST_MODIFIED_TIME);
			psServiceIndexGetEntries = m_session.prepare(SERVICE_INDEX_GET_ENTRIES);
			psServiceIndexInsertModifiedTime = m_session.prepare(SERVICE_INDEX_INSERT_MODIFIED_TIME);
		}
	}

	public void close()
	{
		m_shuttingDown = true;
		m_session.close();
		m_cassandraClient.close();
	}

	public Session getSession()
	{
		return m_session;
	}

	public LoadBalancingPolicy getLoadBalancingPolicy()
	{
		return m_cassandraClient.getWriteLoadBalancingPolicy();
	}

	public String getClusterName()
	{
		return m_cassandraClient.getClusterConfiguration().getClusterName();
	}

	public ResultSet execute(Statement statement)
	{
		return m_session.execute(statement);
	}

	public ResultSetFuture executeAsync(Statement statement)
	{
		return m_session.executeAsync(statement);
	}

	public ConsistencyLevel getReadConsistencyLevel()
	{
		return m_cassandraClient.getClusterConfiguration().getReadConsistencyLevel();
	}

	public ConsistencyLevel getWriteConsistencyLevel()
	{
		return m_cassandraClient.getClusterConfiguration().getWriteConsistencyLevel();
	}

	public String getKeyspace()
	{
		return m_cassandraClient.getKeyspace();
	}

	private void setupSchema(CassandraClient cassandraClient, EnumSet<Type> clusterType)
	{
		try (Session session = cassandraClient.getSession())
		{
			session.execute(String.format(CREATE_KEYSPACE, cassandraClient.getKeyspace(),
					cassandraClient.getReplication()));
		}

		try (Session session = cassandraClient.getKeyspaceSession())
		{
			if (clusterType.contains(Type.WRITE))
			{
				try
				{
					session.execute(DATA_POINTS_TABLE+" "+m_cassandraConfiguration.getCreateWithConfig(DATA_POINTS_TABLE_NAME));
					session.execute(ROW_KEY_INDEX_TABLE+" "+m_cassandraConfiguration.getCreateWithConfig(ROW_KEY_INDEX_TABLE_NAME));
					session.execute(STRING_INDEX_TABLE+" "+m_cassandraConfiguration.getCreateWithConfig(STRING_INDEX_TABLE_NAME));

					session.execute(ROW_KEYS+" "+m_cassandraConfiguration.getCreateWithConfig(ROW_KEYS_NAME));
					session.execute(TAG_INDEXED_ROW_KEYS+" "+m_cassandraConfiguration.getCreateWithConfig(TAG_INDEXED_ROW_KEYS_NAME));
					session.execute(ROW_KEY_TIME_INDEX+" "+m_cassandraConfiguration.getCreateWithConfig(ROW_KEY_TIME_INDEX_NAME));
				}
				catch (Exception e)
				{
					m_readonlyMode = true;
					logger.warn("Unable to create new schema, cluster is in read only mode.  You may need to upgrade to a newer version of Cassandra.", e);
				}
			}

			if (clusterType.contains(Type.META))
			{
				try
				{
					session.execute(SERVICE_INDEX+" "+m_cassandraConfiguration.getCreateWithConfig(SERVICE_INDEX_NAME));
				}
				catch (Exception e)
				{
					m_readonlyMode = true;
					logger.warn("Unable to create new schema, cluster is in read only mode.  You may need to upgrade to a newer version of Cassandra.", e);
				}
			}
		}
	}

	public boolean containRange(long queryStartTime, long queryEndTime)
	{
		return m_cassandraClient.getClusterConfiguration().containRange(queryStartTime, queryEndTime);
	}

	public RowKeyLookup getRowKeyLookupForMetric(String metricName)
	{
		if (m_alwaysUseTagIndexedLookup || m_tagIndexMetricNames.containsKey(metricName))
		{
			logger.debug("Using tag-indexed row key lookup for {}", metricName);
			return m_indexedRowKeyLookup;
		}
		else
		{
			logger.debug("Using standard row key lookup for {}", metricName);
			return m_rowKeyLookup;
		}
	}

	class RowKeysTableLookup implements RowKeyLookup
	{
		public RowKeysTableLookup()
		{
		}

		protected Statement createInsertStatement(DataPointsRowKey rowKey, int rowKeyTtl)
		{
			return
					psRowKeyInsert.bind()
							.setString(0, rowKey.getMetricName())
							.setString(1, DATA_POINTS_TABLE_NAME)
							.setTimestamp(2, new Date(rowKey.getTimestamp()))
							.setString(3, rowKey.getDataType())
							.setMap(4, rowKey.getTags())
							.setInt(5, rowKeyTtl)
							.setIdempotent(true);
		}

		/*
		 This we want to return as it is added to a batch for insert
		 */
		@Override
		public List<Statement> createInsertStatements(DataPointsRowKey rowKey, int rowKeyTtl)
		{
			return ImmutableList.of(createInsertStatement(rowKey, rowKeyTtl));
		}

		protected Statement createDeleteStatement(DataPointsRowKey rowKey)
		{
			return
					psRowKeyDelete.bind()
							.setString(0, rowKey.getMetricName())
							.setString(1, DATA_POINTS_TABLE_NAME)
							.setTimestamp(2, new Date(rowKey.getTimestamp()))
							.setString(3, rowKey.getDataType())
							.setMap(4, rowKey.getTags());
		}

		/*
			This can be done here
		 */
		@Override
		public List<Statement> createDeleteStatements(DataPointsRowKey rowKey)
		{
			return ImmutableList.of(createDeleteStatement(rowKey));
		}


		@Override
		public ListenableFuture<ResultSet> queryRowKeys(String metricName, long rowKeyTimestamp, SetMultimap<String, String> tags)
		{
			BoundStatement statement = psRowKeyQuery.bind()
					.setString(0, metricName)
					.setString(1, DATA_POINTS_TABLE_NAME)
					.setTimestamp(2, new Date(rowKeyTimestamp));

			statement.setConsistencyLevel(getReadConsistencyLevel());
			ResultSetFuture resultSetFuture = executeAsync(statement);
			return resultSetFuture;
		}

		/*@Override
		public RowKeyResultSetProcessor createRowKeyQueryProcessor(String metricName, long rowKeyTimestamp, SetMultimap<String, String> tags)
		{
			return new RowKeyResultSetProcessor()
			{
				@Override
				public List<Statement> getQueryStatements()
				{
					return ImmutableList.of(
							psRowKeyQuery.bind()
									.setString(0, metricName)
									.setTimestamp(1, new Date(rowKeyTimestamp)));
				}

				@Override
				public ResultSet apply(List<ResultSet> input)
				{
					if (input.size() != 1)
					{
						throw new IllegalStateException("Expected exactly 1 result set, got " + input);
					}
					return input.get(0);
				}


			};
		}*/

	}

	class TagIndexedRowKeysTableLookup extends RowKeysTableLookup
	{
		public TagIndexedRowKeysTableLookup()
		{
		}

		@Override
		public List<Statement> createInsertStatements(DataPointsRowKey rowKey, int rowKeyTtl)
		{
			TagSetHash tagSetHash = generateTagPairHashes(rowKey);
			List<Statement> insertStatements = new ArrayList<>(tagSetHash.getTagPairHashes().size());

			//Insert index statements
			insertStatements.addAll(createIndexStatements(rowKey, rowKeyTtl));

			//Always insert into row keys table
			insertStatements.add(createInsertStatement(rowKey, rowKeyTtl));

			return insertStatements;
		}

		@Override
		public List<Statement> createIndexStatements(DataPointsRowKey rowKey, int rowKeyTtl)
		{
			TagSetHash tagSetHash = generateTagPairHashes(rowKey);
			List<Statement> insertStatements = new ArrayList<>(tagSetHash.getTagPairHashes().size());
			Date rowKeyTimestamp = new Date(rowKey.getTimestamp());
			for (String tagPair : tagSetHash.getTagPairHashes())
			{
				insertStatements.add(
						psTagIndexedRowKeyInsert.bind()
								.setString(0, rowKey.getMetricName())
								.setString(1, DATA_POINTS_TABLE_NAME)
								.setTimestamp(2, rowKeyTimestamp)
								.setString(3, rowKey.getDataType())
								.setString(4, tagPair)
								.setInt(5, tagSetHash.getTagCollectionHash())
								.setMap(6, rowKey.getTags())
								.setInt(7, rowKeyTtl)
								.setIdempotent(true));
			}

			return insertStatements;
		}

		@Override
		public List<Statement> createDeleteStatements(DataPointsRowKey rowKey)
		{
			TagSetHash tagSetHash = generateTagPairHashes(rowKey);
			List<Statement> deleteStatements = new ArrayList<>(tagSetHash.getTagPairHashes().size());
			Date rowKeyTimestamp = new Date(rowKey.getTimestamp());
			for (String tagPair : tagSetHash.getTagPairHashes()) {
				deleteStatements.add(
						psTagIndexedRowKeyDelete.bind()
								.setString(0, rowKey.getMetricName())
								.setString(1, DATA_POINTS_TABLE_NAME)
								.setTimestamp(2, rowKeyTimestamp)
								.setString(3, rowKey.getDataType())
								.setString(4, tagPair)
								.setInt(5, tagSetHash.getTagCollectionHash())
								.setMap(6, rowKey.getTags()));
			}

			//Need to delete from row keys table as well
			deleteStatements.add(createDeleteStatement(rowKey));

			return deleteStatements;
		}

		@Override
		public ListenableFuture<ResultSet> queryRowKeys(String metricName, long rowKeyTimestamp, SetMultimap<String, String> tags)
		{
			//Todo: there is probably still to much going on in this method and can likely be simplified
			if (tags.isEmpty())
			{  //Unable to use tag indexes so defaulting to old behavior.
				return super.queryRowKeys(metricName, rowKeyTimestamp, tags);
			}

			ListMultimap<String, Statement> queryStatementByTagName = createQueryStatementsByTagName(metricName, rowKeyTimestamp, tags);

			//we will always get at least 1 because of the above tags.isEmpty check
			if (queryStatementByTagName.size() == 1)
			{
				Statement rowKeyQueryStmt = queryStatementByTagName.values().iterator().next();
				rowKeyQueryStmt.setConsistencyLevel(getReadConsistencyLevel());
				ResultSetFuture resultSetFuture = executeAsync(rowKeyQueryStmt);

				return resultSetFuture;
			}
			else
			{
				List<Statement> queryStatements = new ArrayList<>(queryStatementByTagName.size());
				Multimap<String, Integer> tagNameToStatementIndexes = ArrayListMultimap.create();

				for (Map.Entry<String, Statement> tagNameAndStatementEntry : queryStatementByTagName.entries())
				{
					String tagName = tagNameAndStatementEntry.getKey();
					Statement queryStatement = tagNameAndStatementEntry.getValue();
					tagNameToStatementIndexes.put(tagName, queryStatements.size());
					queryStatements.add(queryStatement);
				}


				List<ListenableFuture<ResultSet>> resultSetForKeyTimeFutures = new ArrayList<>(queryStatements.size());
				for (Statement rowKeyQueryStmt : queryStatements)
				{
					rowKeyQueryStmt.setConsistencyLevel(getReadConsistencyLevel());
					ResultSetFuture resultSetFuture = executeAsync(rowKeyQueryStmt);
					resultSetForKeyTimeFutures.add(resultSetFuture);
				}


				ListenableFuture<ResultSet> keyTimeQueryResultSetFuture =
						Futures.transform(Futures.allAsList(resultSetForKeyTimeFutures), new Function<List<ResultSet>, ResultSet>()
						{
							@Nullable
							@Override
							public ResultSet apply(@Nullable List<ResultSet> input)
							{
								List<List<ResultSet>> resultSetsGroupedByTagName = new ArrayList<>(tagNameToStatementIndexes.size());
								for (Collection<Integer> indexCollection : tagNameToStatementIndexes.asMap().values())
								{
									List<ResultSet> resultSetsForTag = new ArrayList<>(indexCollection.size());
									for (Integer resultSetIndex : indexCollection)
									{
										resultSetsForTag.add(input.get(resultSetIndex));
									}
									resultSetsGroupedByTagName.add(resultSetsForTag);
								}
								Comparator<RowCountEstimatingRowKeyResultSet> comparator =
										Comparator
												.<RowCountEstimatingRowKeyResultSet>comparingInt(r -> r.isEstimated() ? 1 : 0)
												.thenComparing(RowCountEstimatingRowKeyResultSet::getRowCount);
								return resultSetsGroupedByTagName.stream().map(RowCountEstimatingRowKeyResultSet::create).min(comparator)
										.orElseThrow(() -> new IllegalStateException("No minimal ResultSet found"));
							}
						});

				return keyTimeQueryResultSetFuture;
			}
		}

		private ListMultimap<String, Statement> createQueryStatementsByTagName(String metricName, long rowKeyTimestamp, SetMultimap<String, String> tags)
		{
			// Using tag pair hashes as the key in this map can lead to collisions, but that's not a problem
			// because we're simply using the tag pair hashes as an initial AND filter, and further filtering is
			// done on the incoming ResultSets
			Map<String, String> tagPairHashToTagName = new HashMap<>();
			for (Map.Entry<String, String> tagPairEntry : tags.entries())
			{
				tagPairHashToTagName.put(
						hashForTagPair(tagPairEntry.getKey(), tagPairEntry.getValue()),
						tagPairEntry.getKey());
			}

			Date timestamp = new Date(rowKeyTimestamp);
			ListMultimap<String, Statement> queryStatementsByTagName = ArrayListMultimap.create(tagPairHashToTagName.size(), 1);
			for (Map.Entry<String, String> tagPairHashAndTagNameEntry : tagPairHashToTagName.entrySet())
			{
				String tagPair = tagPairHashAndTagNameEntry.getKey();
				String tagName = tagPairHashAndTagNameEntry.getValue();
				queryStatementsByTagName.put(
						tagName,
						psTagIndexedRowKeyQuery.bind()
								.setString(0, metricName)
								.setString(1, DATA_POINTS_TABLE_NAME)
								.setTimestamp(2, timestamp)
								.setString(3, tagPair));
			}
			return queryStatementsByTagName;
		}

		private TagSetHash generateTagPairHashes(DataPointsRowKey rowKey)
		{
			//identify which tags we are indexing on
			Collection<String> indexedTags = m_tagIndexMetricNames.get(rowKey.getMetricName());
			boolean allTags = indexedTags.contains("*");

			//todo add filter on rowKey.getTags().entrySet to limit what tags are indexed based on config
			Hasher tagCollectionHasher = Hashing.murmur3_32().newHasher();
			Set<String> tagPairHashes = new HashSet<>(rowKey.getTags().size());
			for (Map.Entry<String, String> tagPairEntry : rowKey.getTags().entrySet())
			{
				if (m_alwaysUseTagIndexedLookup || allTags || indexedTags.contains(tagPairEntry.getKey()))
				{
					tagPairHashes.add(hashForTagPair(tagPairEntry.getKey(), tagPairEntry.getValue()));
					tagCollectionHasher.putString(tagPairEntry.getKey(), Charsets.UTF_8);
					tagCollectionHasher.putString(tagPairEntry.getValue(), Charsets.UTF_8);
				}
			}
			return new TagSetHash(tagCollectionHasher.hash().asInt(), tagPairHashes);
		}

		private String hashForTagPair(String tagName, String tagValue)
		{
			return new StringBuilder().append(tagName).append('=').append(tagValue).toString();
		}
	}

	/**
	 * Holds a hash of a full set of tag pairs, as well as individual tag pair hashes.
	 */
	private static class TagSetHash {

		private final int tagCollectionHash;
		private final Set<String> tagPairHashes;

		public TagSetHash(int tagCollectionHash, Set<String> tagPairs)
		{
			this.tagCollectionHash = tagCollectionHash;
			this.tagPairHashes = tagPairs;
		}

		public int getTagCollectionHash()
		{
			return tagCollectionHash;
		}

		public Set<String> getTagPairHashes()
		{
			return tagPairHashes;
		}
	}
}
