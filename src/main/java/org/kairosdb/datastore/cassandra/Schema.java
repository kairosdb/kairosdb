package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

/**
 Created by bhawkins on 4/29/17.
 */
public class Schema
{
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

	public static final String ROW_KEY_INDEX_TABLE = "" +
			"CREATE TABLE IF NOT EXISTS row_key_index (\n" +
			"  key blob,\n" +
			"  column1 blob,\n" +
			"  value blob,\n" +
			"  PRIMARY KEY ((key), column1)\n" +
			") WITH COMPACT STORAGE";

	public static final String ROW_KEY_TIME_INDEX = "" +
			"CREATE TABLE IF NOT EXISTS row_key_time_index (\n" +
			"  metric text,\n" +
			"  row_time timestamp,\n" +
			"  value text,\n" +
			"  PRIMARY KEY ((metric), row_time)\n" +
			")";

	public static final String ROW_KEYS = "" +
			"CREATE TABLE IF NOT EXISTS row_keys (\n" +
			"  metric text,\n" +
			"  row_time timestamp,\n" +
			"  data_type text,\n" +
			"  tags frozen<map<text, text>>,\n" +
			"  value text,\n" +
			"  PRIMARY KEY ((metric, row_time), data_type, tags)\n" +
			")";

	public static final String STRING_INDEX_TABLE = "" +
			"CREATE TABLE IF NOT EXISTS string_index (\n" +
			"  key blob,\n" +
			"  column1 text,\n" +
			"  value blob,\n" +
			"  PRIMARY KEY ((key), column1)\n" +
			") WITH COMPACT STORAGE";

	public static final String SERVICE_INDEX = "" +
			"CREATE TABLE IF NOT EXISTS service_index (" +
			" service text," +
			" service_key text," +
			" key text," +
			" value text," +
			" PRIMARY KEY ((service), service_key, key)" +
			")";



	//All inserts and deletes add millisecond timestamp consistency with old code and TWCS instead of nanos
	public static final String DATA_POINTS_INSERT = "INSERT INTO data_points " +
			"(key, column1, value) VALUES (?, ?, ?) USING TTL ? AND TIMESTAMP ?";

	public static final String ROW_KEY_TIME_INSERT = "INSERT INTO row_key_time_index " +
			"(metric, row_time) VALUES (?, ?) USING TTL ? AND TIMESTAMP ?";

	public static final String ROW_KEY_INSERT = "INSERT INTO row_keys " +
			"(metric, row_time, data_type, tags) VALUES (?, ?, ?, ?) USING TTL ?"; // AND TIMESTAMP ?";

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

	public static final String DATA_POINTS_DELETE = "DELETE FROM data_points " +
			"WHERE key = ? AND column1 >= ? AND column1 <= ?";

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
			"FROM row_key_time_index WHERE metric = ? AND " +
			"row_time >= ? AND row_time <= ?";

	public static final String ROW_KEY_QUERY = "SELECT row_time, data_type, tags " +
			"FROM row_keys WHERE metric = ? AND row_time = ?";

	public static final String ROW_KEY_TAG_QUERY_WITH_TYPE = "SELECT row_time, data_type, tags " +
			"FROM row_keys WHERE metric = ? AND row_time = ? AND data_type IN %s"; //Use ValueSequence when setting this

	public static final String ROW_KEY_TIME_DELETE = "DELETE FROM row_key_time_index " +
			"WHERE metric = ? AND row_time = ?";

	public static final String ROW_KEY_DELETE = "DELETE FROM row_keys WHERE metric = ? AND row_time = ?";

	//Service index queries
	public static final String SERVICE_INDEX_INSERT = "INSERT INTO service_index " +
			"(service, service_key, key, value) VALUES (?, ?, ?, ?)";

	public static final String SERVICE_INDEX_GET = "SELECT value, WRITETIME(value) " +
			"FROM service_index WHERE service = ? AND service_key = ? AND key = ?";

	public static final String SERVICE_INDEX_LIST_KEYS = "SELECT key " +
			"FROM service_index WHERE service = ? AND service_key = ? ORDER BY service_key, key ASC";

	public static final String SERVICE_INDEX_LIST_KEYS_PREFIX = "SELECT key " +
			"FROM service_index WHERE service = ? AND service_key = ? AND " +
			"key >= ? AND key < ?";

	public static final String SERVICE_INDEX_LIST_SERVICE_KEYS = "SELECT service_key " +
			"FROM service_index WHERE service = ?";

	public static final String SERVICE_INDEX_DELETE_KEY = "DELETE FROM service_index " +
			"WHERE service = ? AND service_key = ? AND key = ?";

	public final PreparedStatement psDataPointsInsert;
	//public final PreparedStatement m_psInsertRowKey;
	public final PreparedStatement psStringIndexInsert;
	public final PreparedStatement psDataPointsQueryAsc;
	public final PreparedStatement psStringIndexQuery;
	public final PreparedStatement psStringIndexDelete;
	public final PreparedStatement psRowKeyIndexQuery;
	public final PreparedStatement psRowKeyQuery;
	public final PreparedStatement psRowKeyTimeQuery;
	public final PreparedStatement psDataPointsDeleteRow;
	public final PreparedStatement psDataPointsDelete;
	public final PreparedStatement psRowKeyIndexDelete;
	public final PreparedStatement psRowKeyIndexDeleteRow;
	public final PreparedStatement psDataPointsQueryDesc;
	public final PreparedStatement psRowKeyTimeInsert;
	public final PreparedStatement psRowKeyInsert;
	public final PreparedStatement psDataPointsQueryAscLimit;
	public final PreparedStatement psDataPointsQueryDescLimit;
	public final PreparedStatement psServiceIndexInsert;
	public final PreparedStatement psServiceIndexGet;
	public final PreparedStatement psServiceIndexListKeys;
	public final PreparedStatement psServiceIndexListKeysPrefix;
	public final PreparedStatement psServiceIndexListServiceKeys;
	public final PreparedStatement psServiceIndexDeleteKey;
	public final PreparedStatement psRowKeyTimeDelete;
	public final PreparedStatement psRowKeyDelete;

	private final Session m_session;



	public Schema(CassandraClient cassandraClient)
	{
		setupSchema(cassandraClient);

		m_session = cassandraClient.getKeyspaceSession();

		psDataPointsInsert = m_session.prepare(DATA_POINTS_INSERT);
		//m_psInsertRowKey      = m_session.prepare(ROW_KEY_INDEX_INSERT);
		psRowKeyTimeInsert = m_session.prepare(ROW_KEY_TIME_INSERT);
		psRowKeyInsert = m_session.prepare(ROW_KEY_INSERT);
		psStringIndexInsert = m_session.prepare(STRING_INDEX_INSERT);
		psStringIndexQuery = m_session.prepare(STRING_INDEX_QUERY);
		psStringIndexDelete = m_session.prepare(STRING_INDEX_DELETE);
		psDataPointsQueryAsc = m_session.prepare(DATA_POINTS_QUERY_ASC);
		psDataPointsQueryDesc = m_session.prepare(DATA_POINTS_QUERY_DESC);
		psDataPointsQueryAscLimit = m_session.prepare(DATA_POINTS_QUERY_ASC_LIMIT);
		psDataPointsQueryDescLimit = m_session.prepare(DATA_POINTS_QUERY_DESC_LIMIT);
		psRowKeyIndexQuery = m_session.prepare(ROW_KEY_INDEX_QUERY);
		psRowKeyQuery = m_session.prepare(ROW_KEY_QUERY);
		psRowKeyTimeQuery = m_session.prepare(ROW_KEY_TIME_QUERY);
		psRowKeyTimeDelete = m_session.prepare(ROW_KEY_TIME_DELETE);
		psRowKeyDelete = m_session.prepare(ROW_KEY_DELETE);
		psDataPointsDelete = m_session.prepare(DATA_POINTS_DELETE);
		psDataPointsDeleteRow = m_session.prepare(DATA_POINTS_DELETE_ROW);
		psRowKeyIndexDelete = m_session.prepare(ROW_KEY_INDEX_DELETE);
		psRowKeyIndexDeleteRow = m_session.prepare(ROW_KEY_INDEX_DELETE_ROW);

		psServiceIndexInsert = m_session.prepare(SERVICE_INDEX_INSERT);
		psServiceIndexGet = m_session.prepare(SERVICE_INDEX_GET);
		psServiceIndexListKeys = m_session.prepare(SERVICE_INDEX_LIST_KEYS);
		psServiceIndexListKeysPrefix = m_session.prepare(SERVICE_INDEX_LIST_KEYS_PREFIX);
		psServiceIndexListServiceKeys = m_session.prepare(SERVICE_INDEX_LIST_SERVICE_KEYS);
		psServiceIndexDeleteKey = m_session.prepare(SERVICE_INDEX_DELETE_KEY);
	}

	public Session getSession()
	{
		return m_session;
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
			session.execute(ROW_KEYS);
			session.execute(ROW_KEY_TIME_INDEX);
			session.execute(SERVICE_INDEX);
		}
	}
}
