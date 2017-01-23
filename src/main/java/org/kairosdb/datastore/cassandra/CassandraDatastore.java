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

import com.datastax.driver.core.*;
import com.google.common.collect.SetMultimap;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.google.inject.name.Named;
//import com.netflix.astyanax.serializers.StringSerializer;
import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.SliceQuery;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.datapoints.*;
import org.kairosdb.core.datastore.*;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.queue.EventCompletionCallBack;
import org.kairosdb.core.queue.ProcessorHandler;
import org.kairosdb.core.queue.QueueProcessor;
import org.kairosdb.core.reporting.ThreadReporter;
import org.kairosdb.events.DataPointEvent;
import org.kairosdb.util.CongestionExecutorService;
import org.kairosdb.util.KDataInput;
import org.kairosdb.util.MemoryMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.*;

import static com.google.common.base.Preconditions.checkNotNull;

public class CassandraDatastore implements Datastore, ProcessorHandler
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


	//todo desc order for data queries
	public static final String DATA_POINTS_INSERT = "INSERT INTO data_points " +
			"(key, column1, value) VALUES (?, ?, ?) USING TTL ?";

	public static final String ROW_KEY_INDEX_INSERT = "INSERT INTO row_key_index " +
			"(key, column1, value) VALUES (?, ?, 0x00) USING TTL ?";

	public static final String STRING_INDEX_INSERT = "INSERT INTO string_index " +
			"(key, column1, value) VALUES (?, ?, 0x00)";

	public static final String DATA_POINTS_QUERY = "SELECT column1, value FROM data_points WHERE key = ? AND " +
			"column1 >= ? AND column1 < ?";

	public static final String STRING_QUERY = "SELECT column1 FROM string_index " +
			"WHERE key = ?";

	public static final String ROW_KEY_INDEX_QUERY = "SELECT column1 FROM row_key_index " +
			"WHERE key = ? AND column1 >= ? AND column1 < ?";

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


	public static final String CF_DATA_POINTS_NAME = "data_points";
	public static final String CF_ROW_KEY_INDEX_NAME = "row_key_index";
	public static final String CF_STRING_INDEX_NAME = "string_index";

	public static final String ROW_KEY_METRIC_NAMES = "metric_names";
	public static final String ROW_KEY_TAG_NAMES = "tag_names";
	public static final String ROW_KEY_TAG_VALUES = "tag_values";
	private static final Charset UTF_8 = Charset.forName("UTF-8");

	private final Cluster m_cluster;
	private final Keyspace m_keyspace;
	private final EventBus m_eventBus;


	//new properties
	private final CassandraClient m_cassandraClient;
	private final AstyanaxClient m_astyanaxClient;

	private Session m_session;
	private final PreparedStatement m_psInsertData;
	private final PreparedStatement m_psInsertRowKey;
	private final PreparedStatement m_psInsertString;
	private final PreparedStatement m_psQueryData;
	private final PreparedStatement m_psStringQuery;
	private final PreparedStatement m_psRowKeyIndexQuery;
	private final PreparedStatement m_psRowKeyQuery;
	private final PreparedStatement m_psRowKeyTimeQuery;

	private BatchStatement m_batchStatement;
	private final Object m_batchLock = new Object();

	private final boolean m_useThrift;
	//End new props

	private String m_keyspaceName;
	private int m_singleRowReadSize;
	private int m_multiRowSize;
	private int m_multiRowReadSize;

	private DataCache<DataPointsRowKey> m_rowKeyCache = new DataCache<DataPointsRowKey>(1024);
	private DataCache<String> m_metricNameCache = new DataCache<String>(1024);

	private final KairosDataPointFactory m_kairosDataPointFactory;
	private final QueueProcessor m_queueProcessor;
	private final CongestionExecutorService m_congestionExecutor;

	private CassandraConfiguration m_cassandraConfiguration;

	@Inject
	private LongDataPointFactory m_longDataPointFactory = new LongDataPointFactoryImpl();


	@Inject
	public CassandraDatastore(@Named("HOSTNAME") final String hostname,
			CassandraClient cassandraClient,
			AstyanaxClient astyanaxClient,
			CassandraConfiguration cassandraConfiguration,
			HectorConfiguration configuration,
			KairosDataPointFactory kairosDataPointFactory,
			QueueProcessor queueProcessor,
			EventBus eventBus,
			CongestionExecutorService congestionExecutor) throws DatastoreException
	{
		m_cassandraClient = cassandraClient;
		m_astyanaxClient = astyanaxClient;
		m_kairosDataPointFactory = kairosDataPointFactory;
		m_queueProcessor = queueProcessor;
		m_congestionExecutor = congestionExecutor;
		m_eventBus = eventBus;

		m_useThrift = cassandraConfiguration.isUseThrift();

		setupSchema();

		m_session = m_cassandraClient.getKeyspaceSession();
		//Prepare queries

		m_psInsertData = m_session.prepare(DATA_POINTS_INSERT);
		m_psInsertRowKey = m_session.prepare(ROW_KEY_INDEX_INSERT);
		m_psInsertString = m_session.prepare(STRING_INDEX_INSERT);
		m_psQueryData = m_session.prepare(DATA_POINTS_QUERY);
		m_psStringQuery = m_session.prepare(STRING_QUERY);
		m_psRowKeyIndexQuery = m_session.prepare(ROW_KEY_INDEX_QUERY);
		m_psRowKeyQuery = m_session.prepare(ROW_KEY_QUERY);
		m_psRowKeyTimeQuery = m_session.prepare(ROW_KEY_TIME_QUERY);

		m_cassandraConfiguration = cassandraConfiguration;
		m_singleRowReadSize = m_cassandraConfiguration.getSingleRowReadSize();
		m_multiRowSize = m_cassandraConfiguration.getMultiRowSize();
		m_multiRowReadSize = m_cassandraConfiguration.getMultiRowReadSize();
		m_keyspaceName = m_cassandraConfiguration.getKeyspaceName();

		m_rowKeyCache = new DataCache<DataPointsRowKey>(m_cassandraConfiguration.getRowKeyCacheSize());
		m_metricNameCache = new DataCache<String>(m_cassandraConfiguration.getStringCacheSize());

		try
		{
			CassandraHostConfigurator hostConfig = configuration.getConfiguration();
			int threadCount = hostConfig.buildCassandraHosts().length + 3;

			m_cluster = HFactory.getOrCreateCluster("kairosdb-cluster",
					hostConfig, m_cassandraConfiguration.getCassandraAuthentication());

			KeyspaceDefinition keyspaceDef = m_cluster.describeKeyspace(m_keyspaceName);


			//set global consistency level
			ConfigurableConsistencyLevel confConsLevel = new ConfigurableConsistencyLevel();
			confConsLevel.setDefaultReadConsistencyLevel(m_cassandraConfiguration.getDataReadLevel().getHectorLevel());
			confConsLevel.setDefaultWriteConsistencyLevel(m_cassandraConfiguration.getDataWriteLevel().getHectorLevel());

			//create keyspace instance with specified consistency
			m_keyspace = HFactory.createKeyspace(m_keyspaceName, m_cluster, confConsLevel);
		}
		catch (HectorException e)
		{
			throw new DatastoreException(e);
		}


		//This needs to be done last as it tells the processor we are ready for data
		m_queueProcessor.setProcessorHandler(this);
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
		/*if (dataPointEvent.getMetricName().startsWith("blast"))
			return;*/
		m_queueProcessor.put(dataPointEvent);
	}

	@Override
	public void handleEvents(List<DataPointEvent> events, EventCompletionCallBack eventCompletionCallBack)
	{
		BatchHandler batchHandler;

		if (m_useThrift)
			batchHandler = m_astyanaxClient.getBatchHandler(events, eventCompletionCallBack,
					m_cassandraConfiguration.getDatapointTtl(),
					m_rowKeyCache, m_metricNameCache, m_eventBus, m_session,
					m_psInsertData, m_psInsertRowKey, m_psInsertString);
		else
			batchHandler = new CQLBatchHandler(events, eventCompletionCallBack,
					m_cassandraConfiguration.getDatapointTtl(),
					m_rowKeyCache, m_metricNameCache, m_eventBus, m_session,
					m_psInsertData, m_psInsertRowKey, m_psInsertString);

		m_congestionExecutor.submit(batchHandler);
	}


	private Iterable<String> queryStringIndex(final String key)
	{
		BoundStatement boundStatement = new BoundStatement(m_psStringQuery);
		boundStatement.setBytesUnsafe(0, StringSerializer.get().toByteBuffer(key));

		ResultSet resultSet = m_session.execute(boundStatement);

		List<String> ret = new ArrayList<String>();

		while (!resultSet.isExhausted())
		{
			Row row = resultSet.one();
			ret.add(StringSerializer.get().fromByteBuffer(row.getBytes(0)));
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
	public void queryDatabase(DatastoreMetricQuery query, QueryCallback queryCallback) throws DatastoreException
	{
		//queryWithRowKeys(query, queryCallback, getKeysForQueryIterator(query));
		cqlQueryWithRowKeys(query, queryCallback, getKeysForQueryIterator(query));
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
		ExecutorService resultsExecutor = Executors.newSingleThreadExecutor();
		//Controls the number of queries sent out at the same time.
		Semaphore querySemaphor = new Semaphore(100); //todo: add config for this

		while (rowKeys.hasNext())
		{
			rowCount ++;
			DataPointsRowKey rowKey = rowKeys.next();
			//System.out.println("Query for "+rowKey.toString());
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

			BoundStatement boundStatement = new BoundStatement(m_psQueryData);
			boundStatement.setBytesUnsafe(0, DATA_POINTS_ROW_KEY_SERIALIZER.toByteBuffer(rowKey));
			boundStatement.setBytesUnsafe(1, startBuffer);
			boundStatement.setBytesUnsafe(2, endBuffer);

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
			querySemaphor.acquire(100); //todo use same as above
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

	private void queryWithRowKeys(DatastoreMetricQuery query,
			QueryCallback queryCallback, Iterator<DataPointsRowKey> rowKeys)
	{
		long startTime = System.currentTimeMillis();
		long currentTimeTier = 0L;
		String currentType = null;
		int rowCount = 0;

		List<QueryRunner> runners = new ArrayList<QueryRunner>();
		List<DataPointsRowKey> queryKeys = new ArrayList<DataPointsRowKey>();

		MemoryMonitor mm = new MemoryMonitor(20);
		while (rowKeys.hasNext())
		{
			rowCount++;
			DataPointsRowKey rowKey = rowKeys.next();
			if (currentTimeTier == 0L)
				currentTimeTier = rowKey.getTimestamp();

			if (currentType == null)
				currentType = rowKey.getDataType();

			if ((rowKey.getTimestamp() == currentTimeTier) && (queryKeys.size() < m_multiRowSize) &&
					(currentType.equals(rowKey.getDataType())))
			{
				queryKeys.add(rowKey);
			}
			else
			{
				runners.add(new QueryRunner(m_keyspace, CF_DATA_POINTS_NAME, m_kairosDataPointFactory,
						queryKeys,
						query.getStartTime(), query.getEndTime(), queryCallback, m_singleRowReadSize,
						m_multiRowReadSize, query.getLimit(), query.getOrder()));

				queryKeys = new ArrayList<DataPointsRowKey>();
				queryKeys.add(rowKey);
				currentTimeTier = rowKey.getTimestamp();
				currentType = rowKey.getDataType();
			}

			mm.checkMemoryAndThrowException();
		}

		ThreadReporter.addDataPoint(ROW_KEY_COUNT, rowCount);

		//There may be stragglers that are not ran
		if (!queryKeys.isEmpty())
		{
			runners.add(new QueryRunner(m_keyspace, CF_DATA_POINTS_NAME, m_kairosDataPointFactory,
					queryKeys,
					query.getStartTime(), query.getEndTime(), queryCallback, m_singleRowReadSize,
					m_multiRowReadSize, query.getLimit(), query.getOrder()));
		}

		ThreadReporter.addDataPoint(KEY_QUERY_TIME, System.currentTimeMillis() - startTime);

		//Changing the check rate
		mm.setCheckRate(1);
		try
		{
			//TODO: Run this with multiple threads
			for (QueryRunner runner : runners)
			{
				runner.runQuery();

				mm.checkMemoryAndThrowException();
			}

			queryCallback.endDataPoints();
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
				//m_dataPointWriteBuffer.deleteRow(rowKey, now);  // delete the whole row
				//m_rowKeyWriteBuffer.deleteColumn(rowKey.getMetricName(), rowKey, now); // Delete the index
				m_rowKeyCache.clear();
			}
			else
			{
				partialRows.add(rowKey);
			}
		}

		queryWithRowKeys(deleteQuery, new DeletingCallback(deleteQuery.getName()), partialRows.iterator());

		// If index is gone, delete metric name from Strings column family
		if (deleteAll)
		{
			//m_rowKeyWriteBuffer.deleteRow(deleteQuery.getName(), now);
			//todo fix me
			//m_stringIndexWriteBuffer.deleteColumn(ROW_KEY_METRIC_NAMES, deleteQuery.getName(), now);
			m_rowKeyCache.clear();
			m_metricNameCache.clear();
		}
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
				BoundStatement negStatement = new BoundStatement(m_psRowKeyIndexQuery);
				negStatement.setBytesUnsafe(0, StringSerializer.get().toByteBuffer(metricName));
				setStartEndKeys(negStatement, metricName, startTime, -1L);

				ResultSetFuture future = m_session.executeAsync(negStatement);
				futures.add(future);


				BoundStatement posStatement = new BoundStatement(m_psRowKeyIndexQuery);
				posStatement.setBytesUnsafe(0, StringSerializer.get().toByteBuffer(metricName));
				setStartEndKeys(posStatement, metricName, 0L, endTime);

				future = m_session.executeAsync(posStatement);
				futures.add(future);
			}
			else
			{
				BoundStatement statement = new BoundStatement(m_psRowKeyIndexQuery);
				statement.setBytesUnsafe(0, StringSerializer.get().toByteBuffer(metricName));
				setStartEndKeys(statement, metricName, startTime, endTime);

				ResultSetFuture future = m_session.executeAsync(statement);
				futures.add(future);
			}

			//New index query index is broken up by time tier
			List<Long> queryKeyList = createQueryKeyList(metricName, startTime, endTime);
			for (Long keyTime : queryKeyList)
			{
				BoundStatement statement = new BoundStatement(m_psRowKeyQuery);
				statement.setString(0, metricName);
				statement.setTime(1, keyTime);

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
			if (iterator.getColumnDefinitions().contains("metric"))
				newIndex = true;

outer:
			while (!iterator.isExhausted())
			{
				DataPointsRowKey rowKey;
				Row record = iterator.one();

				if (newIndex)
					rowKey = new DataPointsRowKey(m_metricName, record.getTime(0),
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

			BoundStatement statement = new BoundStatement(m_psRowKeyTimeQuery);
			statement.setString(0, metricName);
			statement.setTimestamp(1, new Date(calculateRowTime(startTime)));
			statement.setTimestamp(2, new Date(endTime));

			ResultSet rows = m_session.execute(statement);

			while (!rows.isExhausted())
			{
				ret.add(rows.one().getTime(0));
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
			while (m_currentResultSet != null && !m_currentResultSet.isExhausted())
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

	private class FilteredRowKeyIterator implements Iterator<DataPointsRowKey>
	{
		private ColumnSliceIterator<String, DataPointsRowKey, String> m_sliceIterator;

		/**
		 Used when a query spans positive and negative time values, we have to
		 query the positive separate from the negative times as negative times
		 are sorted after the positive ones.
		 */
		private ColumnSliceIterator<String, DataPointsRowKey, String> m_continueSliceIterator;
		private DataPointsRowKey m_nextKey;
		private SetMultimap<String, String> m_filterTags;

		public FilteredRowKeyIterator(String metricName, long startTime, long endTime,
				SetMultimap<String, String> filterTags)
		{
			m_filterTags = filterTags;
			SliceQuery<String, DataPointsRowKey, String> sliceQuery =
					HFactory.createSliceQuery(m_keyspace, StringSerializer.get(),
							new DataPointsRowKeySerializer(true), StringSerializer.get());

			sliceQuery.setColumnFamily(CF_ROW_KEY_INDEX_NAME)
					.setKey(metricName);

			if ((startTime < 0) && (endTime >= 0))
			{
				m_sliceIterator = createSliceIterator(sliceQuery, metricName,
						startTime, -1L);

				SliceQuery<String, DataPointsRowKey, String> sliceQuery2 =
						HFactory.createSliceQuery(m_keyspace, StringSerializer.get(),
								new DataPointsRowKeySerializer(true), StringSerializer.get());

				sliceQuery2.setColumnFamily(CF_ROW_KEY_INDEX_NAME)
						.setKey(metricName);

				m_continueSliceIterator = createSliceIterator(sliceQuery2, metricName,
						0, endTime);
			}
			else
			{
				m_sliceIterator = createSliceIterator(sliceQuery, metricName,
						startTime, endTime);
			}

		}

		private ColumnSliceIterator<String, DataPointsRowKey, String> createSliceIterator(
				SliceQuery<String, DataPointsRowKey, String> sliceQuery,
				String metricName, long startTime, long endTime)
		{
			DataPointsRowKey startKey = new DataPointsRowKey(metricName,
					calculateRowTime(startTime), "");

			DataPointsRowKey endKey = new DataPointsRowKey(metricName,
					calculateRowTime(endTime), "");
			endKey.setEndSearchKey(true);

			ColumnSliceIterator<String, DataPointsRowKey, String> iterator = new ColumnSliceIterator<String, DataPointsRowKey, String>(sliceQuery,
					startKey, endKey, false, m_singleRowReadSize);

			return (iterator);
		}

		private DataPointsRowKey nextKeyFromIterator(ColumnSliceIterator<String, DataPointsRowKey, String> iterator)
		{
			DataPointsRowKey next = null;

outer:
			while (iterator.hasNext())
			{
				DataPointsRowKey rowKey = iterator.next().getName();

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

		@Override
		public boolean hasNext()
		{
			m_nextKey = nextKeyFromIterator(m_sliceIterator);

			if ((m_nextKey == null) && (m_continueSliceIterator != null))
				m_nextKey = nextKeyFromIterator(m_continueSliceIterator);

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
		private long m_now = System.currentTimeMillis();
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

			//todo fix me
			//m_dataPointWriteBuffer.deleteColumn(m_currentRow, columnName, m_now);
		}

		@Override
		public void startDataPointSet(String dataType, Map<String, String> tags) throws IOException
		{
			m_currentType = dataType;
			m_currentTags = new TreeMap<String, String>(tags);
			//This causes the row key to get reset with the first data point
			m_currentRow = null;
		}

		@Override
		public void endDataPoints()
		{
		}
	}
}
