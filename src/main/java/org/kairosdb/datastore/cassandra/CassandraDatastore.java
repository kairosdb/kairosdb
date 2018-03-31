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
import com.datastax.driver.core.Host;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
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
import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.datastore.QueryPlugin;
import org.kairosdb.core.datastore.ServiceKeyStore;
import org.kairosdb.core.datastore.ServiceKeyValue;
import org.kairosdb.core.datastore.TagSet;
import org.kairosdb.core.datastore.TagSetImpl;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.queue.EventCompletionCallBack;
import org.kairosdb.core.queue.ProcessorHandler;
import org.kairosdb.core.queue.QueueProcessor;
import org.kairosdb.core.reporting.KairosMetricReporter;
import org.kairosdb.core.reporting.ThreadReporter;
import org.kairosdb.eventbus.Subscribe;
import org.kairosdb.events.DataPointEvent;
import org.kairosdb.util.IngestExecutorService;
import org.kairosdb.util.KDataInput;
import org.kairosdb.util.MemoryMonitor;
import org.kairosdb.util.SimpleStatsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Named;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
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
import java.util.concurrent.ThreadFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.kairosdb.datastore.cassandra.CassandraConfiguration.KEYSPACE_PROPERTY;

public class CassandraDatastore implements Datastore, ProcessorHandler, KairosMetricReporter,
		ServiceKeyStore
{
	public static final Logger logger = LoggerFactory.getLogger(CassandraDatastore.class);

	public static final int LONG_FLAG = 0x0;
	public static final int FLOAT_FLAG = 0x1;

	public static final DataPointsRowKeySerializer DATA_POINTS_ROW_KEY_SERIALIZER = new DataPointsRowKeySerializer();


	public static final long ROW_WIDTH = 1814400000L; //3 Weeks wide

	public static final String KEY_QUERY_TIME = "kairosdb.datastore.cassandra.key_query_time";
	public static final String ROW_KEY_COUNT = "kairosdb.datastore.cassandra.row_key_count";
	public static final String RAW_ROW_KEY_COUNT = "kairosdb.datastore.cassandra.raw_row_key_count";


	public static final String ROW_KEY_METRIC_NAMES = "metric_names";
	public static final String ROW_KEY_TAG_NAMES = "tag_names";
	public static final String ROW_KEY_TAG_VALUES = "tag_values";
	private static final Charset UTF_8 = Charset.forName("UTF-8");

	//private final Cluster m_cluster;

	//new properties
	private final CassandraClient m_cassandraClient;
	//private final AstyanaxClient m_astyanaxClient;


	private final Schema m_schema;
	private Session m_session;

	@Inject
	private final BatchStats m_batchStats = new BatchStats();

	@Inject
	private DataCache<DataPointsRowKey> m_rowKeyCache = new DataCache<DataPointsRowKey>(1024);
	@Inject
	private DataCache<String> m_metricNameCache = new DataCache<String>(1024);

	private final KairosDataPointFactory m_kairosDataPointFactory;
	private final QueueProcessor m_queueProcessor;
	private final IngestExecutorService m_congestionExecutor;
	private final CassandraModule.BatchHandlerFactory m_batchHandlerFactory;
	private final CassandraModule.DeleteBatchHandlerFactory m_deleteBatchHandlerFactory;

	private CassandraConfiguration m_cassandraConfiguration;

	@Inject
	private SimpleStatsReporter m_simpleStatsReporter = new SimpleStatsReporter();

	@Inject
	@Named("kairosdb.queue_processor.batch_size")
	private int m_batchSize;  //Used for batching delete requests

	@Inject
	@Named(KEYSPACE_PROPERTY)
	private String m_keyspace = "kairosdb";


	@Inject
	public CassandraDatastore(
			CassandraClient cassandraClient,
			CassandraConfiguration cassandraConfiguration,
			Schema schema,
			Session session,
			KairosDataPointFactory kairosDataPointFactory,
			QueueProcessor queueProcessor,
			IngestExecutorService congestionExecutor,
			CassandraModule.BatchHandlerFactory batchHandlerFactory,
			CassandraModule.DeleteBatchHandlerFactory deleteBatchHandlerFactory) throws DatastoreException
	{
		m_cassandraClient = cassandraClient;
		//m_astyanaxClient = astyanaxClient;
		m_kairosDataPointFactory = kairosDataPointFactory;
		m_queueProcessor = queueProcessor;
		m_congestionExecutor = congestionExecutor;

		m_batchHandlerFactory = batchHandlerFactory;
		m_deleteBatchHandlerFactory = deleteBatchHandlerFactory;

		m_schema = schema;
		m_session = session;

		m_cassandraConfiguration = cassandraConfiguration;

		//This needs to be done last as it tells the processor we are ready for data
		m_queueProcessor.setProcessorHandler(this);
	}

	//Used for creating the end string for prefix searches
	private static ByteBuffer serializeEndString(String str)
	{
		byte[] bytes = str.getBytes(UTF_8);
		bytes[bytes.length-1]++;
		return ByteBuffer.wrap(bytes);
	}

	private static ByteBuffer serializeString(String str)
	{
		return ByteBuffer.wrap(str.getBytes(UTF_8));
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
		m_queueProcessor.shutdown();
		m_session.close();
		m_cassandraClient.close();
	}

	@Subscribe
	public void putDataPoint(DataPointEvent dataPointEvent) throws DatastoreException
	{
		//Todo make sure when shutting down this throws an exception
		checkNotNull(dataPointEvent.getDataPoint().getDataStoreDataType());
		m_queueProcessor.put(dataPointEvent);
	}

	@Override
	public void handleEvents(List<DataPointEvent> events, EventCompletionCallBack eventCompletionCallBack,
			boolean fullBatch)
	{
		BatchHandler batchHandler;

		batchHandler = m_batchHandlerFactory.create(events, eventCompletionCallBack, fullBatch);

		m_congestionExecutor.submit(batchHandler);
	}

	private Iterable<String> queryStringIndex(final String key, final String prefix)
	{
		BoundStatement boundStatement = new BoundStatement(m_schema.psStringIndexPrefixQuery);
		boundStatement.setBytesUnsafe(0, serializeString(key));
		boundStatement.setBytesUnsafe(1, serializeString(prefix));
		boundStatement.setBytesUnsafe(2, serializeEndString(prefix));

		boundStatement.setConsistencyLevel(m_cassandraConfiguration.getDataReadLevel());

		ResultSet resultSet = m_session.execute(boundStatement);

		List<String> ret = new ArrayList<String>();

		while (!resultSet.isExhausted())
		{
			Row row = resultSet.one();
			ret.add(row.getString(0));
		}

		return ret;
	}

	private Iterable<String> queryStringIndex(final String key)
	{
		BoundStatement boundStatement = new BoundStatement(m_schema.psStringIndexQuery);
		boundStatement.setBytesUnsafe(0, serializeString(key));
		boundStatement.setConsistencyLevel(m_cassandraConfiguration.getDataReadLevel());

		ResultSet resultSet = m_session.execute(boundStatement);

		List<String> ret = new ArrayList<String>();

		while (!resultSet.isExhausted())
		{
			Row row = resultSet.one();
			ret.add(row.getString(0));
		}

		return ret;
	}

	@Override
	public Iterable<String> getMetricNames(String prefix)
	{
		if (prefix == null)
			return queryStringIndex(ROW_KEY_METRIC_NAMES);
		else
			return queryStringIndex(ROW_KEY_METRIC_NAMES, prefix);
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
		BoundStatement statement = new BoundStatement(m_schema.psServiceIndexInsert);
		statement.setString(0, service);
		statement.setString(1, serviceKey);
		statement.setString(2, key);
		statement.setString(3, value);

		m_session.execute(statement);
	}

	@Override
	public ServiceKeyValue getValue(String service, String serviceKey, String key) throws DatastoreException
	{
		BoundStatement statement = new BoundStatement(m_schema.psServiceIndexGet);
		statement.setString(0, service);
		statement.setString(1, serviceKey);
		statement.setString(2, key);

		ResultSet resultSet = m_session.execute(statement);
		Row row = resultSet.one();

		if (row != null)
			return new ServiceKeyValue(row.getString(0), new Date(row.getTime(1)));

		return null;
	}

	@Override
	public Iterable<String> listServiceKeys(String service)
			throws DatastoreException
	{
		List<String> ret = new ArrayList<>();

		if (m_schema.psServiceIndexListServiceKeys == null)
		{
			throw new DatastoreException("List Service Keys is not available on this version of Cassandra.");
		}

		BoundStatement statement = new BoundStatement(m_schema.psServiceIndexListServiceKeys);
		statement.setString(0, service);

		ResultSet resultSet = m_session.execute(statement);
		while (!resultSet.isExhausted())
		{
			ret.add(resultSet.one().getString(0));
		}

		return ret;
	}

    @Override
	public Iterable<String> listKeys(String service, String serviceKey) throws DatastoreException
	{
		List<String> ret = new ArrayList<>();

		BoundStatement statement = new BoundStatement(m_schema.psServiceIndexListKeys);
		statement.setString(0, service);
		statement.setString(1, serviceKey);

		ResultSet resultSet = m_session.execute(statement);
		while (!resultSet.isExhausted())
		{
			String key = resultSet.one().getString(0);
			if (key != null) {  // The last row for the primary key doesn't get deleted and has a null key and isExhausted still return false. So check for null
				ret.add(key);
			}
		}

		return ret;
	}

	@Override
	public Iterable<String> listKeys(String service, String serviceKey, String keyStartsWith) throws DatastoreException
	{
		String begin = keyStartsWith;
		String end = keyStartsWith + Character.MAX_VALUE;

		List<String> ret = new ArrayList<>();

		BoundStatement statement = new BoundStatement(m_schema.psServiceIndexListKeysPrefix);
		statement.setString(0, service);
		statement.setString(1, serviceKey);
		statement.setString(2, begin);
		statement.setString(3, end);

		ResultSet resultSet = m_session.execute(statement);
		while (!resultSet.isExhausted())
		{
			String key = resultSet.one().getString(0);
			if (key != null) {  // The last row for the primary key doesn't get deleted and has a null key and isExhausted still return false. So check for null
				ret.add(key);
			}
		}

		return ret;
	}

	@Override
	public void deleteKey(String service, String serviceKey, String key)
			throws DatastoreException
	{
		BoundStatement statement = new BoundStatement(m_schema.psServiceIndexDeleteKey);
		statement.setString(0, service);
		statement.setString(1, serviceKey);
		statement.setString(2, key);

		m_session.execute(statement);

		// Update modification time
		statement = new BoundStatement(m_schema.psServiceIndexInsertModifiedTime);
		statement.setString(0, service);
		statement.setString(1, serviceKey);

		m_session.execute(statement);
	}

	@Override
	public Date getServiceKeyLastModifiedTime(String service, String serviceKey) throws DatastoreException
	{
		BoundStatement statement = new BoundStatement(m_schema.psServiceIndexModificationTime);
		statement.setString(0, service);
		statement.setString(1, serviceKey);

		ResultSet resultSet = m_session.execute(statement);
		Row row = resultSet.one();

		if (row != null)
			return new Date(UUIDs.unixTimestamp(row.getUUID(0)));

		return new Date(0L);
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
		private final Semaphore m_semaphore;  //Used to notify caller when last query is done
		private final QueryMonitor m_queryMonitor;

		public QueryListener(DataPointsRowKey rowKey, QueryCallback callback, Semaphore querySemaphor, QueryMonitor queryMonitor)
		{
			m_rowKey = rowKey;
			m_callback = callback;
			m_semaphore = querySemaphor;
			m_queryMonitor = queryMonitor;
		}

		@Override
		public void onSuccess(@Nullable ResultSet result)
		{
			try
			{
				//CQL will give back results that are empty
				if (result.isExhausted())
					return;

				try (QueryCallback.DataPointWriter dataPointWriter = m_callback.startDataPointSet(m_rowKey.getDataType(), m_rowKey.getTags()))
				{

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
								dataPointWriter.addDataPoint(
										new LegacyLongDataPoint(timestamp,
												ValueSerializer.getLongFromByteBuffer(value)));
							}
							else
							{
								dataPointWriter.addDataPoint(
										new LegacyDoubleDataPoint(timestamp,
												ValueSerializer.getDoubleFromByteBuffer(value)));
							}
						}
						else
						{
							dataPointWriter.addDataPoint(
									dataPointFactory.getDataPoint(timestamp, KDataInput.createInput(value)));
						}

						m_queryMonitor.incrementCounter();

					}
				}

			}
			catch (Exception e)
			{
				logger.error("QueryListener failure", e);
				m_queryMonitor.failQuery(e);
			}
			finally
			{
				m_semaphore.release();
			}
		}

		@Override
		public void onFailure(Throwable t)
		{
			logger.error("Async query failure", t);
			m_queryMonitor.failQuery(t);
			m_semaphore.release();
		}
	}


	private void cqlQueryWithRowKeys(DatastoreMetricQuery query,
			QueryCallback queryCallback, Iterator<DataPointsRowKey> rowKeys) throws DatastoreException
	{
		List<ResultSetFuture> queryResults = new ArrayList<>();
		int rowCount = 0;
		long queryStartTime = query.getStartTime();
		long queryEndTime = query.getEndTime();
		boolean useLimit = query.getLimit() != 0;
		QueryMonitor queryMonitor = new QueryMonitor(m_cassandraConfiguration.getQueryLimit());

		ExecutorService resultsExecutor = Executors.newFixedThreadPool(m_cassandraConfiguration.getQueryReaderThreads(),
				new ThreadFactory()
				{
					private int m_count = 0;
					@Override
					public Thread newThread(Runnable r)
					{
						m_count ++;
						return new Thread(r, "query_"+query.getName()+"-"+m_count);
					}
				});
		//Controls the number of queries sent out at the same time.
		Semaphore querySemaphore = new Semaphore(m_cassandraConfiguration.getSimultaneousQueries());

		while (rowKeys.hasNext())
		{
			rowCount ++;
			int sessionNum = 1; //rowCount % 2;
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
					boundStatement = new BoundStatement(m_schema.psDataPointsQueryAscLimit);
				else
					boundStatement = new BoundStatement(m_schema.psDataPointsQueryDescLimit);
			}
			else
			{
				if (query.getOrder() == Order.ASC)
					boundStatement = new BoundStatement(m_schema.psDataPointsQueryAsc);
				else
					boundStatement = new BoundStatement(m_schema.psDataPointsQueryDesc);
			}

			boundStatement.setBytesUnsafe(0, DATA_POINTS_ROW_KEY_SERIALIZER.toByteBuffer(rowKey));
			boundStatement.setBytesUnsafe(1, startBuffer);
			boundStatement.setBytesUnsafe(2, endBuffer);

			if (useLimit)
				boundStatement.setInt(3, query.getLimit());

			boundStatement.setConsistencyLevel(m_cassandraConfiguration.getDataReadLevel());

			try
			{
				querySemaphore.acquire();
			}
			catch (InterruptedException e)
			{
				e.printStackTrace();
			}

			if (queryMonitor.keepRunning())
			{
				ResultSetFuture resultSetFuture = m_session.executeAsync(boundStatement);
				queryResults.add(resultSetFuture);

				Futures.addCallback(resultSetFuture, new QueryListener(rowKey, queryCallback, querySemaphore, queryMonitor), resultsExecutor);
			}
			else
			{
				//Something broke cancel queries
				for (ResultSetFuture queryResult : queryResults)
				{
					queryResult.cancel(true);
				}

				break;
			}

		}

		ThreadReporter.addDataPoint(ROW_KEY_COUNT, rowCount);

		try
		{
			if (queryMonitor.getException() == null)
				querySemaphore.acquire(m_cassandraConfiguration.getSimultaneousQueries());
			resultsExecutor.shutdown();
		}
		catch (InterruptedException e)
		{
			logger.error("Query interrupted", e);
		}

		if (queryMonitor.getException() != null)
			throw new DatastoreException(queryMonitor.getException());
	}

	private void deletePartialRow(DataPointsRowKey rowKey, long start, long end) throws DatastoreException
	{
		if (m_schema.psDataPointsDeleteRange != null)
		{
			BoundStatement statement = new BoundStatement(m_schema.psDataPointsDeleteRange);
			statement.setBytesUnsafe(0, DATA_POINTS_ROW_KEY_SERIALIZER.toByteBuffer(rowKey));
			ByteBuffer b = ByteBuffer.allocate(4);
			b.putInt(getColumnName(rowKey.getTimestamp(), start));
			b.rewind();
			statement.setBytesUnsafe(1, b);

			b = ByteBuffer.allocate(4);
			b.putInt(getColumnName(rowKey.getTimestamp(), end));
			b.rewind();
			statement.setBytesUnsafe(2, b);

			statement.setConsistencyLevel(m_cassandraConfiguration.getDataReadLevel());
			m_session.executeAsync(statement);
		}
		else
		{
			DatastoreMetricQuery deleteQuery = new QueryMetric(start, end, 0,
					rowKey.getMetricName());

			cqlQueryWithRowKeys(deleteQuery, new DeletingCallback(deleteQuery.getName()),
					Collections.singletonList(rowKey).iterator());
		}
	}


	@Override
	public void deleteDataPoints(DatastoreMetricQuery deleteQuery) throws DatastoreException
	{
		checkNotNull(deleteQuery);
		boolean clearCache = false;


		boolean deleteAll = false;
		if (deleteQuery.getStartTime() == Long.MIN_VALUE && deleteQuery.getEndTime() == Long.MAX_VALUE)
			deleteAll = true;

		Iterator<DataPointsRowKey> rowKeyIterator = getKeysForQueryIterator(deleteQuery);
		List<DataPointsRowKey> partialRows = new ArrayList<DataPointsRowKey>();

		while (rowKeyIterator.hasNext())
		{
			DataPointsRowKey rowKey = rowKeyIterator.next();
			//System.out.println("Deleting from row "+rowKey);
			long rowKeyTimestamp = rowKey.getTimestamp();
			if (deleteQuery.getStartTime() <= rowKeyTimestamp && (deleteQuery.getEndTime() >= rowKeyTimestamp + ROW_WIDTH - 1))
			{
				//System.out.println("Delete entire row");
				BoundStatement statement = new BoundStatement(m_schema.psDataPointsDeleteRow);
				statement.setBytesUnsafe(0, DATA_POINTS_ROW_KEY_SERIALIZER.toByteBuffer(rowKey));
				statement.setConsistencyLevel(m_cassandraConfiguration.getDataReadLevel());
				m_session.execute(statement);

				//Delete from old row keys
				statement = new BoundStatement(m_schema.psRowKeyIndexDelete);
				statement.setBytesUnsafe(0, serializeString(rowKey.getMetricName()));
				statement.setBytesUnsafe(1, DATA_POINTS_ROW_KEY_SERIALIZER.toByteBuffer(rowKey));
				statement.setConsistencyLevel(m_cassandraConfiguration.getDataReadLevel());
				m_session.execute(statement);

				//Delete new row key index
				statement = new BoundStatement(m_schema.psRowKeyDelete);
				statement.setString(0, rowKey.getMetricName());
				statement.setTimestamp(1, new Date(rowKey.getTimestamp()));
				statement.setString(2, rowKey.getDataType());
				statement.setMap(3, rowKey.getTags());
				statement.setConsistencyLevel(m_cassandraConfiguration.getDataReadLevel());
				m_session.execute(statement);


				//Should only remove if the entire time window goes away and no tags are specified in query
				//todo if we allow deletes for specific types this needs to change
				if (deleteQuery.getTags().isEmpty())
				{
					statement = new BoundStatement(m_schema.psRowKeyTimeDelete);
					statement.setString(0, rowKey.getMetricName());
					statement.setTimestamp(1, new Date(rowKey.getTimestamp()));
					statement.setConsistencyLevel(m_cassandraConfiguration.getDataReadLevel());
					m_session.execute(statement);
				}

				clearCache = true;
			}
			else if (deleteQuery.getStartTime() <= rowKeyTimestamp)
			{
				//System.out.println("Delete first of row");
				//Delete first portion of row
				//deletePartialRow(rowKey, 0, getColumnName(rowKeyTimestamp, deleteQuery.getEndTime()));
				deletePartialRow(rowKey, rowKeyTimestamp, deleteQuery.getEndTime());
			}
			else if (deleteQuery.getEndTime() >= rowKeyTimestamp + ROW_WIDTH -1)
			{
				//System.out.println("Delete last of row");
				//Delete last portion of row
				//deletePartialRow(rowKey, getColumnName(rowKeyTimestamp, deleteQuery.getStartTime()),
				//		getColumnName(rowKeyTimestamp, rowKeyTimestamp + ROW_WIDTH - 1));
				deletePartialRow(rowKey, deleteQuery.getStartTime(),
						rowKeyTimestamp + ROW_WIDTH - 1);
			}
			else
			{
				//System.out.println("Delete within a row");
				//Delete within a row
				/*deletePartialRow(rowKey, getColumnName(rowKeyTimestamp, deleteQuery.getStartTime()),
						getColumnName(rowKeyTimestamp, deleteQuery.getEndTime()));*/
				deletePartialRow(rowKey, deleteQuery.getStartTime(),
						deleteQuery.getEndTime());
			}
		}

		// If index is gone, delete metric name from Strings column family
		if (deleteAll)
		{
			//System.out.println("Delete All");
			BoundStatement statement = new BoundStatement(m_schema.psRowKeyIndexDeleteRow);
			statement.setBytesUnsafe(0, serializeString(deleteQuery.getName()));
			statement.setConsistencyLevel(m_cassandraConfiguration.getDataReadLevel());
			m_session.executeAsync(statement);

			//Delete from string index
			statement = new BoundStatement(m_schema.psStringIndexDelete);
			statement.setBytesUnsafe(0, serializeString(ROW_KEY_METRIC_NAMES));
			statement.setBytesUnsafe(1, serializeString(deleteQuery.getName()));
			statement.setConsistencyLevel(m_cassandraConfiguration.getDataReadLevel());
			m_session.executeAsync(statement);

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
		private int m_rawRowKeyCount = 0;
		private Set<DataPointsRowKey> m_returnedKeys;  //keep from returning duplicates, querying old and new indexes


		public CQLFilteredRowKeyIterator(String metricName, long startTime, long endTime,
				SetMultimap<String, String> filterTags) throws DatastoreException
		{
			m_filterTags = filterTags;
			m_metricName = metricName;
			List<ResultSetFuture> futures = new ArrayList<>();
			m_returnedKeys = new HashSet<>();
			long timerStart = System.currentTimeMillis();

			//Legacy key index - index is all in one row
			if ((startTime < 0) && (endTime >= 0))
			{
				BoundStatement negStatement = new BoundStatement(m_schema.psRowKeyIndexQuery);
				negStatement.setBytesUnsafe(0, serializeString(metricName));
				setStartEndKeys(negStatement, metricName, startTime, -1L);
				negStatement.setConsistencyLevel(m_cassandraConfiguration.getDataReadLevel());

				ResultSetFuture future = m_session.executeAsync(negStatement);
				futures.add(future);


				BoundStatement posStatement = new BoundStatement(m_schema.psRowKeyIndexQuery);
				posStatement.setBytesUnsafe(0, serializeString(metricName));
				setStartEndKeys(posStatement, metricName, 0L, endTime);
				posStatement.setConsistencyLevel(m_cassandraConfiguration.getDataReadLevel());

				future = m_session.executeAsync(posStatement);
				futures.add(future);
			}
			else
			{
				BoundStatement statement = new BoundStatement(m_schema.psRowKeyIndexQuery);
				statement.setBytesUnsafe(0, serializeString(metricName));
				setStartEndKeys(statement, metricName, startTime, endTime);
				statement.setConsistencyLevel(m_cassandraConfiguration.getDataReadLevel());

				ResultSetFuture future = m_session.executeAsync(statement);
				futures.add(future);
			}

			//System.out.println();
			//New index query index is broken up by time tier
			List<Long> queryKeyList = createQueryKeyList(metricName, startTime, endTime);
			for (Long keyTime : queryKeyList)
			{
				BoundStatement statement = new BoundStatement(m_schema.psRowKeyQuery);
				statement.setString(0, metricName);
				statement.setTimestamp(1, new Date(keyTime));
				statement.setConsistencyLevel(m_cassandraConfiguration.getDataReadLevel());

				//printHosts(m_loadBalancingPolicy.newQueryPlan(m_keyspace, statement));

				ResultSetFuture future = m_session.executeAsync(statement);
				futures.add(future);
			}

			ListenableFuture<List<ResultSet>> listListenableFuture = Futures.allAsList(futures);

			try
			{
				m_resultSets = listListenableFuture.get().iterator();
				if (m_resultSets.hasNext())
					m_currentResultSet = m_resultSets.next();

				ThreadReporter.addDataPoint(KEY_QUERY_TIME, System.currentTimeMillis() - timerStart);
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
				{
					if (record.getString(1) == null)
						continue; //empty row

					rowKey = new DataPointsRowKey(m_metricName, record.getTimestamp(0).getTime(),
							record.getString(1), new TreeMap<String, String>(record.getMap(2, String.class, String.class)));
				}
				else
					rowKey = DATA_POINTS_ROW_KEY_SERIALIZER.fromByteBuffer(record.getBytes(0));

				m_rawRowKeyCount ++;

				Map<String, String> keyTags = rowKey.getTags();
				for (String tag : m_filterTags.keySet())
				{
					String value = keyTags.get(tag);
					if (value == null || !m_filterTags.get(tag).contains(value))
						continue outer; //Don't want this key
				}

				/* We can get duplicate keys from querying old and new indexes */
				if (m_returnedKeys.contains(rowKey))
					continue;

				m_returnedKeys.add(rowKey);
				next = rowKey;
				break;
			}

			return (next);
		}

		private List<Long> createQueryKeyList(String metricName,
				long startTime, long endTime)
		{
			List<Long> ret = new ArrayList<>();

			BoundStatement statement = new BoundStatement(m_schema.psRowKeyTimeQuery);
			statement.setString(0, metricName);
			statement.setTimestamp(1, new Date(calculateRowTime(startTime)));
			statement.setTimestamp(2, new Date(endTime));
			statement.setConsistencyLevel(m_cassandraConfiguration.getDataReadLevel());

			//printHosts(m_loadBalancingPolicy.newQueryPlan(m_keyspace, statement));

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

			if (m_nextKey == null)
			{
				ThreadReporter.addDataPoint(RAW_ROW_KEY_COUNT, m_rawRowKeyCount);
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
		private String m_metricName;

		public DeletingCallback(String metricName)
		{
			m_metricName = metricName;
		}


		@Override
		public DataPointWriter startDataPointSet(String dataType, SortedMap<String, String> tags) throws IOException
		{
			return new DeleteDatePointWriter(dataType, tags);
		}

		private class DeleteDatePointWriter implements DataPointWriter
		{
			private String m_dataType;
			private SortedMap<String, String> m_tags;
			private List<DataPoint> m_dataPoints;

			public DeleteDatePointWriter(String dataType, SortedMap<String, String> tags)
			{
				m_dataType = dataType;
				m_tags = tags;
				m_dataPoints = new ArrayList<>();

			}

			@Override
			public void addDataPoint(DataPoint datapoint) throws IOException
			{
				m_dataPoints.add(datapoint);

				if (m_dataPoints.size() > m_batchSize)
				{
					List<DataPoint> dataPoints = m_dataPoints;
					m_dataPoints = new ArrayList<DataPoint>();

					DeleteBatchHandler deleteBatchHandler = m_deleteBatchHandlerFactory.create(
							m_metricName, m_tags, dataPoints, s_dontCareCallBack);

					m_congestionExecutor.submit(deleteBatchHandler);
				}
			}

			@Override
			public void close() throws IOException
			{
				if (m_dataPoints.size() != 0)
				{
					DeleteBatchHandler deleteBatchHandler = m_deleteBatchHandlerFactory.create(
							m_metricName, m_tags, m_dataPoints, s_dontCareCallBack);

					m_congestionExecutor.submit(deleteBatchHandler);
				}
			}
		}
	}

	private void printHosts(Iterator<Host> hostIterator)
	{
		StringBuilder sb = new StringBuilder();

		while (hostIterator.hasNext())
		{
			sb.append(hostIterator.next().toString()).append(" ");
		}
		System.out.println(sb.toString());
	}

	private static final IDontCareCallBack s_dontCareCallBack = new IDontCareCallBack();

	private static class IDontCareCallBack implements EventCompletionCallBack
	{
		@Override
		public void complete() {} //Dont care
	}

}
