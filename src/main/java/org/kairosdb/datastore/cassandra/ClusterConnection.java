package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;

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

	public static final String DATA_POINTS_TABLE = "" +
			"CREATE TABLE IF NOT EXISTS data_points (\n" +
			"  key blob,\n" +
			"  column1 blob,\n" +
			"  value blob,\n" +
			"  PRIMARY KEY ((key), column1)\n" +
			") WITH COMPACT STORAGE";

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

	public static final String ROW_KEY_INDEX_TABLE = "" +
			"CREATE TABLE IF NOT EXISTS row_key_index (\n" +
			"  key blob,\n" +
			"  column1 blob,\n" +
			"  value blob,\n" +
			"  PRIMARY KEY ((key), column1)\n" +
			")";

	public static final String ROW_KEY_TIME_INDEX = "" +
			"CREATE TABLE IF NOT EXISTS row_key_time_index (\n" +
			"  metric text,\n" +
			"  table_name text,\n" +
			"  row_time timestamp,\n" +
			"  value text,\n" +
			"  PRIMARY KEY ((metric), table_name, row_time)\n" +
			")";

	public static final String ROW_KEYS = "" +
			"CREATE TABLE IF NOT EXISTS row_keys (\n" +
			"  metric text,\n" +
			"  table_name text, \n" +
			"  row_time timestamp,\n" +
			"  data_type text,\n" +
			"  tags frozen<map<text, text>>,\n" +
			"  mtime timeuuid static,\n" +
			"  value text,\n" +
			"  PRIMARY KEY ((metric, table_name, row_time), data_type, tags)\n" +
			")";

	public static final String STRING_INDEX_TABLE = "" +
			"CREATE TABLE IF NOT EXISTS string_index (\n" +
			"  key blob,\n" +
			"  column1 text,\n" +
			"  value blob,\n" +
			"  PRIMARY KEY ((key), column1)\n" +
			")";

	public static final String SERVICE_INDEX = "" +
			"CREATE TABLE IF NOT EXISTS service_index (" +
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
			"(metric, table_name, row_time) VALUES (?, 'data_points', ?) USING TTL ?";

	public static final String ROW_KEY_INSERT = "INSERT INTO row_keys " +
			"(metric, table_name, row_time, data_type, tags, mtime) VALUES (?, 'data_points', ?, ?, ?, now()) USING TTL ?"; // AND TIMESTAMP ?";

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
			"FROM row_key_time_index WHERE metric = ? AND table_name = 'data_points' AND " +
			"row_time >= ? AND row_time <= ?";

	public static final String ROW_KEY_QUERY = "SELECT row_time, data_type, tags " +
			"FROM row_keys WHERE metric = ? AND table_name = 'data_points' AND row_time = ?";

	public static final String ROW_KEY_TAG_QUERY_WITH_TYPE = "SELECT row_time, data_type, tags " +
			"FROM row_keys WHERE metric = ? AND table_name = 'data_points' AND row_time = ? AND data_type IN %s"; //Use ValueSequence when setting this

	public static final String ROW_KEY_TIME_DELETE = "DELETE FROM row_key_time_index " +
			"WHERE metric = ? AND table_name = 'data_points' AND row_time = ?";

	public static final String ROW_KEY_DELETE = "DELETE FROM row_keys WHERE metric = ? " +
			"AND table_name = 'data_points' AND row_time = ? AND data_type = ? " +
			"AND tags = ?";

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
	public PreparedStatement psRowKeyTimeQuery;
	public PreparedStatement psDataPointsDeleteRow;
	public PreparedStatement psDataPointsDeleteRange;
	public PreparedStatement psRowKeyIndexDelete;
	public PreparedStatement psRowKeyIndexDeleteRow;
	public PreparedStatement psDataPointsQueryDesc;
	public PreparedStatement psRowKeyTimeInsert;
	public PreparedStatement psRowKeyInsert;
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
	public PreparedStatement psServiceIndexModificationTime;
	public PreparedStatement psServiceIndexInsertModifiedTime;
	public PreparedStatement psServiceIndexGetEntries;
	public PreparedStatement psDataPointsDelete;

	private Session m_session;
	private final CassandraClient m_cassandraClient;
	private boolean m_readonlyMode;
	private final EnumSet<Type> m_clusterType;
	private volatile boolean m_shuttingDown = false;


	public ClusterConnection(CassandraClient cassandraClient, EnumSet<Type> clusterType)
	{
		m_cassandraClient = cassandraClient;
		m_clusterType = clusterType;
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
			psRowKeyDelete = m_session.prepare(ROW_KEY_DELETE);
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
					session.execute(DATA_POINTS_TABLE);
					session.execute(ROW_KEY_INDEX_TABLE);
					session.execute(STRING_INDEX_TABLE);

					session.execute(ROW_KEYS);
					session.execute(ROW_KEY_TIME_INDEX);
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
					session.execute(SERVICE_INDEX);
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
}
