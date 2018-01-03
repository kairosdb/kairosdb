package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 Created by bhawkins on 4/29/17.
 */
public class ClusterConnection
{
	public static final Logger logger = LoggerFactory.getLogger(ClusterConnection.class);

	public static final String CREATE_KEYSPACE = "" +
			"CREATE KEYSPACE IF NOT EXISTS %s" +
			"  WITH REPLICATION = {'class': 'SimpleStrategy'," +
			"  'replication_factor' : 1}";

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

	public static final String ROW_KEY_DELETE = "DELETE FROM row_keys WHERE metric = ? AND table_name = 'data_points' AND row_time = ?";

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

	public final PreparedStatement psDataPointsInsert;
	//public final PreparedStatement m_psInsertRowKey;
	public final PreparedStatement psStringIndexInsert;
	public final PreparedStatement psDataPointsQueryAsc;
	public final PreparedStatement psStringIndexQuery;
	public final PreparedStatement psStringIndexDelete;
	public final PreparedStatement psRowKeyIndexQuery;
	public PreparedStatement psRowKeyQuery;
	public PreparedStatement psRowKeyTimeQuery;
	public final PreparedStatement psDataPointsDeleteRow;
	public PreparedStatement psDataPointsDeleteRange;
	public final PreparedStatement psRowKeyIndexDelete;
	public final PreparedStatement psRowKeyIndexDeleteRow;
	public final PreparedStatement psDataPointsQueryDesc;
	public PreparedStatement psRowKeyTimeInsert;
	public PreparedStatement psRowKeyInsert;
	public final PreparedStatement psDataPointsQueryAscLimit;
	public final PreparedStatement psDataPointsQueryDescLimit;
	public PreparedStatement psServiceIndexInsert;
	public PreparedStatement psServiceIndexGet;
	public PreparedStatement psServiceIndexListKeys;
	public PreparedStatement psServiceIndexListKeysPrefix;
	public PreparedStatement psServiceIndexListServiceKeys;
	public PreparedStatement psServiceIndexDeleteKey;
	public PreparedStatement psRowKeyTimeDelete;
	public PreparedStatement psRowKeyDelete;
	public final PreparedStatement psDataPointsDelete;

	private final Session m_session;
	private final CassandraClient m_cassandraClient;
	private boolean m_readonlyMode;


	public ClusterConnection(CassandraClient cassandraClient)
	{
		setupSchema(cassandraClient);

		m_session = cassandraClient.getKeyspaceSession();

		m_cassandraClient = cassandraClient;

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

		psStringIndexInsert = m_session.prepare(STRING_INDEX_INSERT);
		psStringIndexQuery = m_session.prepare(STRING_INDEX_QUERY);
		psStringIndexDelete = m_session.prepare(STRING_INDEX_DELETE);


		if (!m_readonlyMode)
		{
			psRowKeyQuery = m_session.prepare(ROW_KEY_QUERY);
			psRowKeyInsert = m_session.prepare(ROW_KEY_INSERT);
			psRowKeyDelete = m_session.prepare(ROW_KEY_DELETE);
			psRowKeyTimeQuery = m_session.prepare(ROW_KEY_TIME_QUERY);
			psRowKeyTimeDelete = m_session.prepare(ROW_KEY_TIME_DELETE);
			psRowKeyTimeInsert = m_session.prepare(ROW_KEY_TIME_INSERT);

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
		}


	}

	public void close()
	{
		m_session.close();
		m_cassandraClient.close();
	}

	public Session getSession()
	{
		return m_session;
	}

	public LoadBalancingPolicy getLoadBalancingPolicy()
	{
		return m_cassandraClient.getLoadBalancingPolicy();
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

	private void setupSchema(CassandraClient cassandraClient)
	{
		try (Session session = cassandraClient.getSession())
		{
			session.execute(String.format(CREATE_KEYSPACE, cassandraClient.getKeyspace()));
		}

		try (Session session = cassandraClient.getKeyspaceSession())
		{
			session.execute(DATA_POINTS_TABLE);
			session.execute(ROW_KEY_INDEX_TABLE);
			session.execute(STRING_INDEX_TABLE);

			try
			{
				session.execute(ROW_KEYS);
				session.execute(ROW_KEY_TIME_INDEX);
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
