/*
 * Copyright 2016 KairosDB Authors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.google.common.collect.SetMultimap;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.datapoints.DataPointFactory;
import org.kairosdb.core.datapoints.LegacyDataPointFactory;
import org.kairosdb.core.datapoints.LegacyDoubleDataPoint;
import org.kairosdb.core.datapoints.LegacyLongDataPoint;
import org.kairosdb.core.datastore.DataPointRow;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.datastore.DatastoreMetricQuery;
import org.kairosdb.core.datastore.Order;
import org.kairosdb.core.datastore.QueryCallback;
import org.kairosdb.core.datastore.QueryPlugin;
import org.kairosdb.core.datastore.TagSet;
import org.kairosdb.core.datastore.TagSetImpl;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.queue.EventCompletionCallBack;
import org.kairosdb.core.queue.ProcessorHandler;
import org.kairosdb.core.queue.QueueProcessor;
import org.kairosdb.core.reporting.KairosMetricReporter;
import org.kairosdb.core.reporting.ThreadReporter;
import org.kairosdb.events.DataPointEvent;
import org.kairosdb.util.IngestExecutorService;
import org.kairosdb.util.KDataInput;
import org.kairosdb.util.MemoryMonitor;
import org.kairosdb.util.SimpleStatsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import static com.google.common.base.Preconditions.checkNotNull;

public class CassandraDatastore implements Datastore, ProcessorHandler, KairosMetricReporter
{
	public static final Logger logger = LoggerFactory.getLogger(CassandraDatastore.class);


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
			"  column1 blob,\n" +
			"  value blob,\n" +
			"  PRIMARY KEY ((key), column1)\n" +
			") WITH COMPACT STORAGE";

	public static final String SERVICE_INDEX = "" +
			"CREATE TABLE IF NOT EXISTS service_index (" +
			" service text," +
			" service_key text," +
			" key text," +
			" value text," +
			" PRIMARY KEY ((service, service_key), key)" +
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

	public static final String DATA_POINTS_QUERY = "SELECT column1, value FROM data_points WHERE key = ? AND " +
			"column1 >= ? AND column1 < ? ORDER BY column1";

	public static final String DATA_POINTS_QUERY_ASC = DATA_POINTS_QUERY+" ASC";
	public static final String DATA_POINTS_QUERY_DESC = DATA_POINTS_QUERY+" DESC";

	public static final String DATA_POINTS_QUERY_ASC_LIMIT = DATA_POINTS_QUERY_ASC+" LIMIT ?";
	public static final String DATA_POINTS_QUERY_DESC_LIMIT = DATA_POINTS_QUERY_DESC+" LIMIT ?";

	public static final String DATA_POINTS_DELETE = "DELETE FROM data_points " +
			"WHERE key = ? AND column1 = ?";

	public static final String DATA_POINTS_DELETE_ROW = "DELETE FROM data_points " +
			"WHERE key = ?";

	public static final String STRING_INDEX_QUERY = "SELECT column1 FROM string_index " +
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

	public static final int LONG_FLAG = 0x0;
	public static final int FLOAT_FLAG = 0x1;

	public static final DataPointsRowKeySerializer DATA_POINTS_ROW_KEY_SERIALIZER = new DataPointsRowKeySerializer();


	public static final long ROW_WIDTH = 1814400000L; //3 Weeks wide

	public static final String KEY_QUERY_TIME = "kairosdb.datastore.cassandra.key_query_time";
	public static final String ROW_KEY_COUNT = "kairosdb.datastore.cassandra.row_key_count";


	public static final String ROW_KEY_METRIC_NAMES = "metric_names";
	public static final String ROW_KEY_TAG_NAMES = "tag_names";
	public static final String ROW_KEY_TAG_VALUES = "tag_values";
	private static final Charset UTF_8 = Charset.forName("UTF-8");

	//private final Cluster m_cluster;
	private final EventBus m_eventBus;


	//new properties
	private final CassandraClient m_cassandraClient;
	//private final AstyanaxClient m_astyanaxClient;


	private final PreparedStatements m_preparedStatements;
	private Session m_session;
	private LoadBalancingPolicy m_loadBalancingPolicy;


	public class PreparedStatements
	{
		public final PreparedStatement psDataPointsInsert;
		//public final PreparedStatement m_psInsertRowKey;
		public final PreparedStatement psStringIndexInsert;
		public final PreparedStatement psDataPointsQueryAsc;
		public final PreparedStatement psStringIndexQuery;
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

		public PreparedStatements()
		{
			psDataPointsInsert  = m_session.prepare(DATA_POINTS_INSERT);
			//m_psInsertRowKey      = m_session.prepare(ROW_KEY_INDEX_INSERT);
			psRowKeyTimeInsert = m_session.prepare(ROW_KEY_TIME_INSERT);
			psRowKeyInsert = m_session.prepare(ROW_KEY_INSERT);
			psStringIndexInsert = m_session.prepare(STRING_INDEX_INSERT);
			psDataPointsQueryAsc = m_session.prepare(DATA_POINTS_QUERY_ASC);
			psDataPointsQueryDesc = m_session.prepare(DATA_POINTS_QUERY_DESC);
			psDataPointsQueryAscLimit = m_session.prepare(DATA_POINTS_QUERY_ASC_LIMIT);
			psDataPointsQueryDescLimit = m_session.prepare(DATA_POINTS_QUERY_DESC_LIMIT);
			psStringIndexQuery = m_session.prepare(STRING_INDEX_QUERY);
			psRowKeyIndexQuery  = m_session.prepare(ROW_KEY_INDEX_QUERY);
			psRowKeyQuery       = m_session.prepare(ROW_KEY_QUERY);
			psRowKeyTimeQuery   = m_session.prepare(ROW_KEY_TIME_QUERY);
			psDataPointsDelete = m_session.prepare(DATA_POINTS_DELETE);
			psDataPointsDeleteRow = m_session.prepare(DATA_POINTS_DELETE_ROW);
			psRowKeyIndexDelete = m_session.prepare(ROW_KEY_INDEX_DELETE);
			psRowKeyIndexDeleteRow = m_session.prepare(ROW_KEY_INDEX_DELETE_ROW);
		}
	}



	private final BatchStats m_batchStats = new BatchStats();

	private DataCache<DataPointsRowKey> m_rowKeyCache = new DataCache<DataPointsRowKey>(1024);
	private DataCache<String> m_metricNameCache = new DataCache<String>(1024);

	private final KairosDataPointFactory m_kairosDataPointFactory;
	private final QueueProcessor m_queueProcessor;
	private final IngestExecutorService m_congestionExecutor;

	private CassandraConfiguration m_cassandraConfiguration;

	@Inject
	private SimpleStatsReporter m_simpleStatsReporter = new SimpleStatsReporter();


	@Inject
	public CassandraDatastore(@Named("HOSTNAME") final String hostname,
			CassandraClient cassandraClient,
			CassandraConfiguration cassandraConfiguration,
			KairosDataPointFactory kairosDataPointFactory,
			QueueProcessor queueProcessor,
			EventBus eventBus,
			IngestExecutorService congestionExecutor) throws DatastoreException
	{
		m_cassandraClient = cassandraClient;
		//m_astyanaxClient = astyanaxClient;
		m_kairosDataPointFactory = kairosDataPointFactory;
		m_queueProcessor = queueProcessor;
		m_congestionExecutor = congestionExecutor;
		m_eventBus = eventBus;

		setupSchema();

		m_session = m_cassandraClient.getKeyspaceSession();
		m_loadBalancingPolicy = m_cassandraClient.getLoadBalancingPolicy();
		//Prepare queries
		m_preparedStatements = new PreparedStatements();

		m_cassandraConfiguration = cassandraConfiguration;

		m_rowKeyCache = new DataCache<DataPointsRowKey>(m_cassandraConfiguration.getRowKeyCacheSize());
		m_metricNameCache = new DataCache<String>(m_cassandraConfiguration.getStringCacheSize());

		//This needs to be done last as it tells the processor we are ready for data
		m_queueProcessor.setProcessorHandler(this);
	}

	private static ByteBuffer serializeString(String str)
	{
		return ByteBuffer.wrap(str.getBytes(UTF_8));
	}

	private void setupSchema()
	{
		try (Session session = m_cassandraClient.getSession())
		{
			session.execute(String.format(CREATE_KEYSPACE, m_cassandraClient.getKeyspace()));
		}

		try (Session session = m_cassandraClient.getKeyspaceSession())
		{
			session.execute(DATA_POINTS_TABLE);
			session.execute(ROW_KEY_INDEX_TABLE);
			session.execute(STRING_INDEX_TABLE);
			session.execute(ROW_KEYS);
			session.execute(ROW_KEY_TIME_INDEX);
			session.execute(SERVICE_INDEX);
		}
	}


	public void cleanRowKeyCache()
	{
		long currentRow = calculateRowTime(System.currentTimeMillis());

		Set<DataPointsRowKey> keys = m_rowKeyCache.getCachedKeys();

		for (DataPointsRowKey key : keys)
		{
			if (key.getTimestamp() != currentRow)
			{
				m_rowKeyCache.removeKey(key);
			}
		}
	}

	@Override
	public void close() throws InterruptedException
	{
		m_eventBus.unregister(this);

		m_queueProcessor.shutdown();
		m_session.close();
		m_cassandraClient.close();
	}

	@Subscribe
	public void putDataPoint(DataPointEvent dataPointEvent) throws DatastoreException
	{
		m_queueProcessor.put(dataPointEvent);
	}

	@Override
	public void handleEvents(List<DataPointEvent> events, EventCompletionCallBack eventCompletionCallBack,
			boolean fullBatch)
	{
		BatchHandler batchHandler;

		batchHandler = new BatchHandler(events, eventCompletionCallBack,
				m_cassandraConfiguration.getDatapointTtl(),
				m_cassandraConfiguration.getDataWriteLevel(),
				m_rowKeyCache, m_metricNameCache, m_eventBus, m_session,
				m_preparedStatements, fullBatch, m_batchStats, m_loadBalancingPolicy);

		m_congestionExecutor.submit(batchHandler);
	}


	private Iterable<String> queryStringIndex(final String key)
	{
		BoundStatement boundStatement = new BoundStatement(m_preparedStatements.psStringIndexQuery);
		boundStatement.setBytesUnsafe(0, serializeString(key));
		boundStatement.setConsistencyLevel(m_cassandraConfiguration.getDataReadLevel());

		ResultSet resultSet = m_session.execute(boundStatement);

		List<String> ret = new ArrayList<String>();

		while (!resultSet.isExhausted())
		{
			Row row = resultSet.one();
			ret.add(UTF_8.decode(row.getBytes(0)).toString());
		}

		return ret;
	}

	@Override
	public Iterable<String> getMetricNames()
	{
		return queryStringIndex(ROW_KEY_METRIC_NAMES);
	}

	@Override
	public Iterable<String> getTagNames()
	{
		return queryStringIndex(ROW_KEY_TAG_NAMES);
	}

	@Override
	public Iterable<String> getTagValues()
	{
		return queryStringIndex(ROW_KEY_TAG_VALUES);
	}

	@Override
	public TagSet queryMetricTags(DatastoreMetricQuery query) throws DatastoreException
	{
		TagSetImpl tagSet = new TagSetImpl();
		Iterator<DataPointsRowKey> rowKeys = getKeysForQueryIterator(query);

		MemoryMonitor mm = new MemoryMonitor(20);
		while (rowKeys.hasNext())
		{
			DataPointsRowKey dataPointsRowKey = rowKeys.next();
			for (Map.Entry<String, String> tag : dataPointsRowKey.getTags().entrySet())
			{
				tagSet.addTag(tag.getKey(), tag.getValue());
				mm.checkMemoryAndThrowException();
			}
		}

		return (tagSet);
	}

	@Override
	public void setValue(String service, String serviceKey, String key, String value) throws DatastoreException
	{

	}

	@Override
	public String getValue(String service, String serviceKey, String key) throws DatastoreException
	{
		return null;
	}

    @Override
    public Iterable<String> listServiceKeys(String service)
            throws DatastoreException
    {
        return null;
    }

    @Override
	public Iterable<String> listKeys(String service, String serviceKey) throws DatastoreException
	{
		return null;
	}

	@Override
	public Iterable<String> listKeys(String service, String serviceKey, String keyStartsWith) throws DatastoreException
	{
		return null;
	}

    @Override
    public void deleteKey(String service, String serviceKey, String key)
            throws DatastoreException
    {
    }

    @Override
	public void queryDatabase(DatastoreMetricQuery query, QueryCallback queryCallback) throws DatastoreException
	{
		cqlQueryWithRowKeys(query, queryCallback, getKeysForQueryIterator(query));
	}

	@Override
	public List<DataPointSet> getMetrics(long now)
	{
		List<DataPointSet> ret = new ArrayList<>();

		m_simpleStatsReporter.reportStats(m_batchStats.getNameStats(), now,
				"kairosdb.datastore.cassandra.write_batch_size",
				"table", "string_index", ret);
		m_simpleStatsReporter.reportStats(m_batchStats.getDataPointStats(), now,
				"kairosdb.datastore.cassandra.write_batch_size",
				"table", "data_points", ret);
		m_simpleStatsReporter.reportStats(m_batchStats.getRowKeyStats(), now,
				"kairosdb.datastore.cassandra.write_batch_size",
				"table", "row_keys", ret);

		return ret;
	}

	private class QueryListener implements FutureCallback<ResultSet>
	{
		private final DataPointsRowKey m_rowKey;
		private final QueryCallback m_callback;
		private final Semaphore m_semaphore;

		public QueryListener(DataPointsRowKey rowKey, QueryCallback callback, Semaphore querySemaphor)
		{
			m_rowKey = rowKey;
			m_callback = callback;
			m_semaphore = querySemaphor;
		}

		@Override
		public void onSuccess(@Nullable ResultSet result)
		{
			try
			{
				m_callback.startDataPointSet(m_rowKey.getDataType(), m_rowKey.getTags());

				DataPointFactory dataPointFactory = null;
				dataPointFactory = m_kairosDataPointFactory.getFactoryForDataStoreType(m_rowKey.getDataType());

				while (!result.isExhausted())
				{
					Row row = result.one();
					ByteBuffer bytes = row.getBytes(0);

					int columnTime = bytes.getInt();

					ByteBuffer value = row.getBytes(1);
					long timestamp = getColumnTimestamp(m_rowKey.getTimestamp(), columnTime);

					//If type is legacy type it will point to the same object, no need for equals
					if (m_rowKey.getDataType() == LegacyDataPointFactory.DATASTORE_TYPE)
					{
						if (isLongValue(columnTime))
						{
							m_callback.addDataPoint(
									new LegacyLongDataPoint(timestamp,
											ValueSerializer.getLongFromByteBuffer(value)));
						}
						else
						{
							m_callback.addDataPoint(
									new LegacyDoubleDataPoint(timestamp,
											ValueSerializer.getDoubleFromByteBuffer(value)));
						}
					}
					else
					{
						m_callback.addDataPoint(
								dataPointFactory.getDataPoint(timestamp, KDataInput.createInput(value)));
					}

				}

			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
			finally
			{
				m_semaphore.release();
			}
		}

		@Override
		public void onFailure(Throwable t)
		{
			System.out.println(t);
			m_semaphore.release();
		}
	}


	private void cqlQueryWithRowKeys(DatastoreMetricQuery query,
			QueryCallback queryCallback, Iterator<DataPointsRowKey> rowKeys)
	{
		long timerStart = System.currentTimeMillis();
		List<ResultSetFuture> queryResults = new ArrayList<>();
		int rowCount = 0;
		long queryStartTime = query.getStartTime();
		long queryEndTime = query.getEndTime();
		boolean useLimit = query.getLimit() != 0;

		//todo add memory monitor

		ExecutorService resultsExecutor = Executors.newSingleThreadExecutor();
		//Controls the number of queries sent out at the same time.
		Semaphore querySemaphor = new Semaphore(m_cassandraConfiguration.getSimultaneousQueries());

		while (rowKeys.hasNext())
		{
			rowCount ++;
			DataPointsRowKey rowKey = rowKeys.next();
			long tierRowTime = rowKey.getTimestamp();
			int startTime;
			int endTime;
			if (queryStartTime < tierRowTime)
				startTime = 0;
			else
				startTime = getColumnName(tierRowTime, queryStartTime);

			if (queryEndTime > (tierRowTime + ROW_WIDTH))
				endTime = getColumnName(tierRowTime, tierRowTime + ROW_WIDTH) +1;
			else
				endTime = getColumnName(tierRowTime, queryEndTime) +1; //add 1 so we get 0x1 for last bit

			ByteBuffer startBuffer = ByteBuffer.allocate(4);
			startBuffer.putInt(startTime);
			startBuffer.rewind();

			ByteBuffer endBuffer = ByteBuffer.allocate(4);
			endBuffer.putInt(endTime);
			endBuffer.rewind();

			BoundStatement boundStatement;
			if (useLimit)
			{
				if (query.getOrder() == Order.ASC)
					boundStatement = new BoundStatement(m_preparedStatements.psDataPointsQueryAscLimit);
				else
					boundStatement = new BoundStatement(m_preparedStatements.psDataPointsQueryDescLimit);
			}
			else
			{
				if (query.getOrder() == Order.ASC)
					boundStatement = new BoundStatement(m_preparedStatements.psDataPointsQueryAsc);
				else
					boundStatement = new BoundStatement(m_preparedStatements.psDataPointsQueryDesc);
			}

			boundStatement.setBytesUnsafe(0, DATA_POINTS_ROW_KEY_SERIALIZER.toByteBuffer(rowKey));
			boundStatement.setBytesUnsafe(1, startBuffer);
			boundStatement.setBytesUnsafe(2, endBuffer);

			if (useLimit)
				boundStatement.setInt(3, query.getLimit());

			boundStatement.setConsistencyLevel(m_cassandraConfiguration.getDataReadLevel());

			try
			{
				querySemaphor.acquire();
			}
			catch (InterruptedException e)
			{
				e.printStackTrace();
			}
			ResultSetFuture resultSetFuture = m_session.executeAsync(boundStatement);

			Futures.addCallback(resultSetFuture, new QueryListener(rowKey, queryCallback, querySemaphor), resultsExecutor);
		}

		ThreadReporter.addDataPoint(KEY_QUERY_TIME, System.currentTimeMillis() - timerStart);
		ThreadReporter.addDataPoint(ROW_KEY_COUNT, rowCount);

		try
		{
			querySemaphor.acquire(m_cassandraConfiguration.getSimultaneousQueries());
			queryCallback.endDataPoints();
			resultsExecutor.shutdown();
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}


	@Override
	public void deleteDataPoints(DatastoreMetricQuery deleteQuery) throws DatastoreException
	{
		checkNotNull(deleteQuery);
		boolean clearCache = false;

		long now = System.currentTimeMillis();

		boolean deleteAll = false;
		if (deleteQuery.getStartTime() == Long.MIN_VALUE && deleteQuery.getEndTime() == Long.MAX_VALUE)
			deleteAll = true;

		Iterator<DataPointsRowKey> rowKeyIterator = getKeysForQueryIterator(deleteQuery);
		List<DataPointsRowKey> partialRows = new ArrayList<DataPointsRowKey>();

		while (rowKeyIterator.hasNext())
		{
			DataPointsRowKey rowKey = rowKeyIterator.next();
			long rowKeyTimestamp = rowKey.getTimestamp();
			if (deleteQuery.getStartTime() <= rowKeyTimestamp && (deleteQuery.getEndTime() >= rowKeyTimestamp + ROW_WIDTH - 1))
			{
				//todo fix me
				BoundStatement statement = new BoundStatement(m_preparedStatements.psDataPointsDeleteRow);
				statement.setBytesUnsafe(0, DATA_POINTS_ROW_KEY_SERIALIZER.toByteBuffer(rowKey));
				statement.setConsistencyLevel(m_cassandraConfiguration.getDataReadLevel());
				m_session.executeAsync(statement);

				statement = new BoundStatement(m_preparedStatements.psRowKeyIndexDelete);
				statement.setBytesUnsafe(0, serializeString(rowKey.getMetricName()));
				statement.setBytesUnsafe(1, DATA_POINTS_ROW_KEY_SERIALIZER.toByteBuffer(rowKey));
				statement.setConsistencyLevel(m_cassandraConfiguration.getDataReadLevel());
				m_session.executeAsync(statement);
				clearCache = true;
			}
			else
			{
				partialRows.add(rowKey);
			}
		}


		cqlQueryWithRowKeys(deleteQuery, new DeletingCallback(deleteQuery.getName()), partialRows.iterator());

		// If index is gone, delete metric name from Strings column family
		if (deleteAll)
		{
			BoundStatement statement = new BoundStatement(m_preparedStatements.psRowKeyIndexDeleteRow);
			statement.setBytesUnsafe(0, serializeString(deleteQuery.getName()));
			statement.setConsistencyLevel(m_cassandraConfiguration.getDataReadLevel());
			m_session.executeAsync(statement);

			//todo fix me
			//m_stringIndexWriteBuffer.deleteColumn(ROW_KEY_METRIC_NAMES, deleteQuery.getName(), now);
			clearCache = true;
			m_metricNameCache.clear();
		}


		if (clearCache)
			m_rowKeyCache.clear();
	}

	private SortedMap<String, String> getTags(DataPointRow row)
	{
		TreeMap<String, String> map = new TreeMap<String, String>();
		for (String name : row.getTagNames())
		{
			map.put(name, row.getTagValue(name));
		}

		return map;
	}

	/**
	 * Returns the row keys for the query in tiers ie grouped by row key timestamp
	 *
	 * @param query query
	 * @return row keys for the query
	 */
	public Iterator<DataPointsRowKey> getKeysForQueryIterator(DatastoreMetricQuery query) throws DatastoreException
	{
		Iterator<DataPointsRowKey> ret = null;

		List<QueryPlugin> plugins = query.getPlugins();

		//First plugin that works gets it.
		for (QueryPlugin plugin : plugins)
		{
			if (plugin instanceof CassandraRowKeyPlugin)
			{
				ret = ((CassandraRowKeyPlugin) plugin).getKeysForQueryIterator(query);
				break;
			}
		}

		//Default to old behavior if no plugin was provided
		if (ret == null)
		{
			//todo use Iterable.concat to query multiple metrics at the same time.
			//each filtered iterator will be combined into one and returned.
			//one issue is that the queries are done in the constructor
			//would like to do them lazily but would have to through an exception through
			//hasNext call, ick
			ret = new CQLFilteredRowKeyIterator(query.getName(), query.getStartTime(),
					query.getEndTime(), query.getTags());
		}


		return (ret);
	}

	public static long calculateRowTime(long timestamp)
	{
		return (timestamp - (Math.abs(timestamp) % ROW_WIDTH));
	}


	/**
	 This is just for the delete operation of old data points.
	 @param rowTime
	 @param timestamp
	 @param isInteger
	 @return
	 */
	@SuppressWarnings("PointlessBitwiseExpression")
	private static int getColumnName(long rowTime, long timestamp, boolean isInteger)
	{
		int ret = (int) (timestamp - rowTime);

		if (isInteger)
			return ((ret << 1) | LONG_FLAG);
		else
			return ((ret << 1) | FLOAT_FLAG);

	}

	@SuppressWarnings("PointlessBitwiseExpression")
	public static int getColumnName(long rowTime, long timestamp)
	{
		int ret = (int) (timestamp - rowTime);

		/*
			The timestamp is shifted to support legacy datapoints that
			used the extra bit to determine if the value was long or double
		 */
		return (ret << 1);
	}

	public static long getColumnTimestamp(long rowTime, int columnName)
	{
		return (rowTime + (long) (columnName >>> 1));
	}

	public static boolean isLongValue(int columnName)
	{
		return ((columnName & 0x1) == LONG_FLAG);
	}


	private class CQLFilteredRowKeyIterator implements Iterator<DataPointsRowKey>
	{
		private final SetMultimap<String, String> m_filterTags;
		private DataPointsRowKey m_nextKey;
		private final Iterator<ResultSet> m_resultSets;
		private ResultSet m_currentResultSet;
		private final String m_metricName;


		public CQLFilteredRowKeyIterator(String metricName, long startTime, long endTime,
				SetMultimap<String, String> filterTags) throws DatastoreException
		{
			m_filterTags = filterTags;
			m_metricName = metricName;
			List<ResultSetFuture> futures = new ArrayList<>();

			//Legacy key index - index is all in one row
			if ((startTime < 0) && (endTime >= 0))
			{
				BoundStatement negStatement = new BoundStatement(m_preparedStatements.psRowKeyIndexQuery);
				negStatement.setBytesUnsafe(0, serializeString(metricName));
				setStartEndKeys(negStatement, metricName, startTime, -1L);
				negStatement.setConsistencyLevel(m_cassandraConfiguration.getDataReadLevel());

				ResultSetFuture future = m_session.executeAsync(negStatement);
				futures.add(future);


				BoundStatement posStatement = new BoundStatement(m_preparedStatements.psRowKeyIndexQuery);
				posStatement.setBytesUnsafe(0, serializeString(metricName));
				setStartEndKeys(posStatement, metricName, 0L, endTime);
				posStatement.setConsistencyLevel(m_cassandraConfiguration.getDataReadLevel());

				future = m_session.executeAsync(posStatement);
				futures.add(future);
			}
			else
			{
				BoundStatement statement = new BoundStatement(m_preparedStatements.psRowKeyIndexQuery);
				statement.setBytesUnsafe(0, serializeString(metricName));
				setStartEndKeys(statement, metricName, startTime, endTime);
				statement.setConsistencyLevel(m_cassandraConfiguration.getDataReadLevel());

				ResultSetFuture future = m_session.executeAsync(statement);
				futures.add(future);
			}

			//New index query index is broken up by time tier
			List<Long> queryKeyList = createQueryKeyList(metricName, startTime, endTime);
			for (Long keyTime : queryKeyList)
			{
				BoundStatement statement = new BoundStatement(m_preparedStatements.psRowKeyQuery);
				statement.setString(0, metricName);
				statement.setTimestamp(1, new Date(keyTime));
				statement.setConsistencyLevel(m_cassandraConfiguration.getDataReadLevel());

				ResultSetFuture future = m_session.executeAsync(statement);
				futures.add(future);
			}

			ListenableFuture<List<ResultSet>> listListenableFuture = Futures.allAsList(futures);

			try
			{
				m_resultSets = listListenableFuture.get().iterator();
				if (m_resultSets.hasNext())
					m_currentResultSet = m_resultSets.next();
			}
			catch (InterruptedException e)
			{
				throw new DatastoreException("Index query interrupted", e);
			}
			catch (ExecutionException e)
			{
				throw new DatastoreException("Failed to read key index", e);
			}
		}

		private DataPointsRowKey nextKeyFromIterator(ResultSet iterator)
		{
			DataPointsRowKey next = null;
			boolean newIndex = false;
			if (iterator.getColumnDefinitions().contains("row_time"))
				newIndex = true;

outer:
			while (!iterator.isExhausted())
			{
				DataPointsRowKey rowKey;
				Row record = iterator.one();

				if (newIndex)
					rowKey = new DataPointsRowKey(m_metricName, record.getTimestamp(0).getTime(),
							record.getString(1), new TreeMap<String, String>(record.getMap(2, String.class, String.class)));
				else
					rowKey = DATA_POINTS_ROW_KEY_SERIALIZER.fromByteBuffer(record.getBytes(0));

				Map<String, String> keyTags = rowKey.getTags();
				for (String tag : m_filterTags.keySet())
				{
					String value = keyTags.get(tag);
					if (value == null || !m_filterTags.get(tag).contains(value))
						continue outer; //Don't want this key
				}

				next = rowKey;
				break;
			}

			return (next);
		}

		private List<Long> createQueryKeyList(String metricName,
				long startTime, long endTime)
		{
			List<Long> ret = new ArrayList<>();

			BoundStatement statement = new BoundStatement(m_preparedStatements.psRowKeyTimeQuery);
			statement.setString(0, metricName);
			statement.setTimestamp(1, new Date(calculateRowTime(startTime)));
			statement.setTimestamp(2, new Date(endTime));
			statement.setConsistencyLevel(m_cassandraConfiguration.getDataReadLevel());

			ResultSet rows = m_session.execute(statement);

			while (!rows.isExhausted())
			{
				ret.add(rows.one().getTimestamp(0).getTime());
			}

			return ret;
		}

		private void setStartEndKeys(
				BoundStatement boundStatement,
				String metricName, long startTime, long endTime)
		{
			DataPointsRowKey startKey = new DataPointsRowKey(metricName,
					calculateRowTime(startTime), "");

			DataPointsRowKey endKey = new DataPointsRowKey(metricName,
					calculateRowTime(endTime), "");
			endKey.setEndSearchKey(true);

			boundStatement.setBytesUnsafe(1, DATA_POINTS_ROW_KEY_SERIALIZER.toByteBuffer(startKey));
			boundStatement.setBytesUnsafe(2, DATA_POINTS_ROW_KEY_SERIALIZER.toByteBuffer(endKey));
		}

		@Override
		public boolean hasNext()
		{
			m_nextKey = null;
			while (m_currentResultSet != null && (!m_currentResultSet.isExhausted() || m_resultSets.hasNext()))
			{
				m_nextKey = nextKeyFromIterator(m_currentResultSet);

				if (m_nextKey != null)
					break;

				if (m_resultSets.hasNext())
					m_currentResultSet = m_resultSets.next();
			}

			return (m_nextKey != null);
		}

		@Override
		public DataPointsRowKey next()
		{
			return m_nextKey;
		}

		@Override
		public void remove()
		{
		}
	}


	private class DeletingCallback implements QueryCallback
	{
		private SortedMap<String, String> m_currentTags;
		private DataPointsRowKey m_currentRow;
		private final String m_metric;
		private String m_currentType;

		public DeletingCallback(String metric)
		{
			m_metric = metric;
		}


		@Override
		public void addDataPoint(DataPoint datapoint) throws IOException
		{
			long time = datapoint.getTimestamp();

			long rowTime = calculateRowTime(time);
			if (m_currentRow == null)
			{
				m_currentRow = new DataPointsRowKey(m_metric, rowTime, m_currentType, m_currentTags);
			}

			int columnName;
			//Handle old column name format.
			//We get the type after it has been translated from "" to kairos_legacy
			if (m_currentType.equals(LegacyDataPointFactory.DATASTORE_TYPE))
			{
				columnName = getColumnName(rowTime, time, datapoint.isLong());
			}
			else
				columnName = getColumnName(rowTime, time);

			//Todo: may want to send these off in batches
			BoundStatement statement = new BoundStatement(m_preparedStatements.psDataPointsDelete);
			statement.setBytesUnsafe(0, DATA_POINTS_ROW_KEY_SERIALIZER.toByteBuffer(m_currentRow));
			ByteBuffer b = ByteBuffer.allocate(4);
			b.putInt(columnName);
			b.rewind();
			statement.setBytesUnsafe(1, b);
			statement.setConsistencyLevel(m_cassandraConfiguration.getDataWriteLevel());
			m_session.executeAsync(statement);
		}

		@Override
		public void startDataPointSet(String dataType, Map<String, String> tags) throws IOException
		{
			m_currentType = dataType;
			m_currentTags = new TreeMap<String, String>(tags);
			//This causes the row key to get clear with the first data point
			m_currentRow = null;
		}

		@Override
		public void endDataPoints()
		{
		}
	}
}
