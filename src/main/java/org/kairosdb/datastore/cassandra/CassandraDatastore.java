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

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.uuid.Uuids;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.time.Instant;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.KairosPostConstructInit;
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
import org.kairosdb.eventbus.Subscribe;
import org.kairosdb.events.DataPointEvent;
import org.kairosdb.metrics4j.MetricSourceManager;
import org.kairosdb.util.IngestExecutorService;
import org.kairosdb.util.KDataInput;
import org.kairosdb.util.MemoryMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import jakarta.inject.Named;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
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
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;

import static java.util.Objects.requireNonNull;
import static org.kairosdb.datastore.cassandra.ClusterConnection.DATA_POINTS_TABLE_NAME;

public class CassandraDatastore implements Datastore, ProcessorHandler,
		ServiceKeyStore, KairosPostConstructInit
{
	public static final Logger logger = LoggerFactory.getLogger(CassandraDatastore.class);
	public static final CassandraStats stats = MetricSourceManager.getSource(CassandraStats.class);

	public static final int LONG_FLAG = 0x0;
	public static final int FLOAT_FLAG = 0x1;

	public static final DataPointsRowKeySerializer DATA_POINTS_ROW_KEY_SERIALIZER = new DataPointsRowKeySerializer();


	//public static final long ROW_WIDTH = 1814400000L; //3 Weeks wide
	public static final long MAX_CQL_BATCH_SIZE = 10000;

	public static final String KEY_QUERY_TIME = "kairosdb.datastore.cassandra.key_query_time";
	public static final String ROW_KEY_COUNT = "kairosdb.datastore.cassandra.row_key_count";
	public static final String RAW_ROW_KEY_COUNT = "kairosdb.datastore.cassandra.raw_row_key_count";


	public static final String ROW_KEY_METRIC_NAMES = "metric_names";
	public static final String ROW_KEY_TAG_NAMES = "tag_names";
	public static final String ROW_KEY_TAG_VALUES = "tag_values";
	private static final Charset UTF_8 = StandardCharsets.UTF_8;


	private final ClusterConnection m_writeCluster;
	private final ClusterConnection m_metaCluster;
	private final List<ClusterConnection> m_readClusters;
	private final CassandraModule.CQLBatchFactory m_cqlBatchFactory;
	private final Map<String, ClusterConnection> m_clusterMap;

	@Inject
	private DataCache<DataPointsRowKey> m_rowKeyCache = new DataCache<DataPointsRowKey>(1024);
	@Inject
	private DataCache<TimedString> m_metricNameCache = new DataCache<>(1024);

	private final KairosDataPointFactory m_kairosDataPointFactory;
	private final QueueProcessor m_queueProcessor;
	private final IngestExecutorService m_congestionExecutor;
	private final CassandraModule.BatchHandlerFactory m_batchHandlerFactory;
	private final CassandraModule.DeleteBatchHandlerFactory m_deleteBatchHandlerFactory;
	private final CassandraModule.CQLFilteredRowKeyIteratorFactory m_rowKeyFilterFactory;

	private final CassandraConfiguration m_cassandraConfiguration;

	@Inject
	@Named("kairosdb.queue_processor.batch_size")
	private int m_batchSize;  //Used for batching delete requests


	@Inject
	public CassandraDatastore(
			CassandraConfiguration cassandraConfiguration,
			@Named("write_cluster") ClusterConnection writeCluster,
			@Named("meta_cluster") ClusterConnection metaCluster,
			List<ClusterConnection> readClusters,
			KairosDataPointFactory kairosDataPointFactory,
			QueueProcessor queueProcessor,
			IngestExecutorService congestionExecutor,
			CassandraModule.BatchHandlerFactory batchHandlerFactory,
			CassandraModule.DeleteBatchHandlerFactory deleteBatchHandlerFactory,
			CassandraModule.CQLFilteredRowKeyIteratorFactory rowKeyFilterFactory,
			CassandraModule.CQLBatchFactory cqlBatchFactory
			) throws DatastoreException
	{
		//m_astyanaxClient = astyanaxClient;
		m_kairosDataPointFactory = kairosDataPointFactory;
		m_queueProcessor = queueProcessor;
		m_congestionExecutor = congestionExecutor;

		m_batchHandlerFactory = batchHandlerFactory;
		m_deleteBatchHandlerFactory = deleteBatchHandlerFactory;
		m_rowKeyFilterFactory = rowKeyFilterFactory;

		m_writeCluster = writeCluster;
		m_metaCluster = metaCluster;
		m_readClusters = readClusters;

		m_cqlBatchFactory = cqlBatchFactory;

		ImmutableMap.Builder<String, ClusterConnection> builder = ImmutableMap.builder();
		builder.put(m_writeCluster.getClusterName(), m_writeCluster);

		for (ClusterConnection readCluster : readClusters)
		{
			builder.put(readCluster.getClusterName(), readCluster);
		}

		m_clusterMap = builder.build();

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

	public static ByteBuffer serializeString(String str)
	{
		return ByteBuffer.wrap(str.getBytes(UTF_8));
	}


	public void cleanRowKeyCache()
	{
		RowSpec rowSpec = m_writeCluster.getRowSpec();
		if (rowSpec == null)
			return; //This can be ran by the scheduler before we have a connection to Cassandra

		long currentRow = rowSpec.calculateRowTime(System.currentTimeMillis());

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
		m_writeCluster.close();
		for (ClusterConnection readCluster : m_readClusters)
		{
			readCluster.close();
		}
	}

	@Subscribe
	public void putDataPoint(DataPointEvent dataPointEvent) throws DatastoreException
	{
		//Todo make sure when shutting down this throws an exception
		requireNonNull(dataPointEvent.getDataPoint().getDataStoreDataType());
		m_queueProcessor.put(dataPointEvent);
	}

	@Override
	public void handleEvents(List<DataPointEvent> events, EventCompletionCallBack eventCompletionCallBack,
			boolean fullBatch)
	{
		BatchHandler batchHandler;

		batchHandler = m_batchHandlerFactory.create(events, eventCompletionCallBack, fullBatch, m_writeCluster.getRowSpec());

		m_congestionExecutor.submit(batchHandler);
	}

	@Override
	public void init()
	{
	}

	private interface ClusterCallback
	{
		CompletionStage<AsyncResultSet> query(ClusterConnection connection) throws DatastoreException;
	}

	private interface ClusterCallbackList
	{
		List<ListenableFuture<ResultSet>> query(ClusterConnection connection) throws DatastoreException;
	}

	private List<CompletableFuture<AsyncResultSet>> queryClustersWithCollector(ClusterCallback cb, AsyncResultCollector.RowProcessor rowProcessor) throws DatastoreException
	{
		AsyncResultCollector collector = new AsyncResultCollector();

		CompletionStage<AsyncResultSet> resultSetFuture = cb.query(m_writeCluster);
		collector.addResultSet(resultSetFuture, rowProcessor);

		for (ClusterConnection readCluster : m_readClusters)
		{
			CompletionStage<AsyncResultSet> future = cb.query(readCluster);
			if (future != null)
			{
				collector.addResultSet(future, rowProcessor);
			}
		}

		return collector.getFutures();
	}

	private void iterateClusters(ClusterCallback cb) throws DatastoreException
	{
		cb.query(m_writeCluster);

		for (ClusterConnection readCluster : m_readClusters)
		{
			cb.query(readCluster);
		}
	}


	private Iterable<String> queryStringIndex(final String key, final String prefix) throws DatastoreException
	{
		Set<String> ret = new TreeSet<String>();

		List<CompletableFuture<AsyncResultSet>> completionStages = queryClustersWithCollector((cluster) -> {
			BoundStatement boundStatement = cluster.psStringIndexPrefixQuery.boundStatementBuilder()
					.setBytesUnsafe(0, serializeString(key))
					.setBytesUnsafe(1, serializeString(prefix))
					.setBytesUnsafe(2, serializeEndString(prefix))
					.setConsistencyLevel(cluster.getReadConsistencyLevel())
					.build();
			return cluster.executeAsync(boundStatement);
		}, row -> ret.add(row.getString(0)));

		//todo what does a failure look like here??
		completionStages.forEach(CompletableFuture::join);

		/*try
		{
			Iterator<ResultSet> iterator = listListenableFuture.get().iterator();
			while (iterator.hasNext())
			{
				ResultSet resultSet = iterator.next();
				while (!resultSet.isExhausted())
				{
					Row row = resultSet.one();
					ret.add(row.getString(0));
				}
			}
		}
		catch (Exception e)
		{
			throw new DatastoreException("CQL Query failure", e);
		}*/

		return ret;
	}

	private Iterable<String> queryStringIndex(final String key) throws DatastoreException
	{
		//We want the results to be sorted so we use a tree set
		Set<String> ret = new TreeSet<String>();

		List<CompletableFuture<AsyncResultSet>> completionStages = queryClustersWithCollector((cluster) -> {
			BoundStatement boundStatement = cluster.psStringIndexQuery.boundStatementBuilder()
					.setBytesUnsafe(0, serializeString(key))
					.setConsistencyLevel(cluster.getReadConsistencyLevel())
					.build();
			return cluster.executeAsync(boundStatement);
		}, row -> ret.add(row.getString(0)));

		//todo what does a failure look like here??
		completionStages.forEach(CompletableFuture::join);

		return ret;
	}

	@Override
	public Iterable<String> getMetricNames(String prefix) throws DatastoreException
	{
		if (prefix == null)
			return queryStringIndex(ROW_KEY_METRIC_NAMES);
		else
			return queryStringIndex(ROW_KEY_METRIC_NAMES, prefix);
	}

	@Override
	public Iterable<String> getTagNames() throws DatastoreException
	{
		return queryStringIndex(ROW_KEY_TAG_NAMES);
	}

	@Override
	public Iterable<String> getTagValues() throws DatastoreException
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
	public void indexMetricTags(DatastoreMetricQuery query) throws DatastoreException
	{
		CQLBatch batch = m_cqlBatchFactory.create();
		Iterator<DataPointsRowKey> rowKeys = getKeysForQueryIterator(query);

		MemoryMonitor mm = new MemoryMonitor(20);
		long indexStatementCount = 0;
		while (rowKeys.hasNext())
		{
			DataPointsRowKey dataPointsRowKey = rowKeys.next();
			batch.indexRowKey(dataPointsRowKey, dataPointsRowKey.getTtl());
			mm.checkMemoryAndThrowException();
			indexStatementCount++;
			if (indexStatementCount % MAX_CQL_BATCH_SIZE == 0) {
				batch.submitBatch();
				batch = m_cqlBatchFactory.create();
			}
		}
		batch.submitBatch();
	}

	@Override
	public long getMinTimeValue()
	{
		return Long.MIN_VALUE;
	}

	@Override
	public long getMaxTimeValue()
	{
		return Long.MAX_VALUE;
	}

	@Override
	public void setValue(String service, String serviceKey, String key, String value) throws DatastoreException
	{
		BoundStatement statement = m_metaCluster.psServiceIndexInsert.boundStatementBuilder()
				.setString(0, service)
				.setString(1, serviceKey)
				.setString(2, key)
				.setString(3, value)
				.setConsistencyLevel(m_metaCluster.getWriteConsistencyLevel())
				.build();

		m_metaCluster.execute(statement);
	}

	@Override
	public ServiceKeyValue getValue(String service, String serviceKey, String key) throws DatastoreException
	{
		BoundStatement statement = m_metaCluster.psServiceIndexGet.boundStatementBuilder()
				.setString(0, service)
				.setString(1, serviceKey)
				.setString(2, key)
				.setConsistencyLevel(m_metaCluster.getReadConsistencyLevel())
				.build();

		ResultSet resultSet = m_metaCluster.execute(statement);
		Row row = resultSet.one();

		if (row != null)
			return new ServiceKeyValue(row.getString(0), new Date(row.getInstant(1).toEpochMilli()));

		return null;
	}

	@Override
	public Iterable<String> listServiceKeys(String service)
			throws DatastoreException
	{
		List<String> ret = new ArrayList<>();

		if (m_metaCluster.psServiceIndexListServiceKeys == null)
		{
			throw new DatastoreException("List Service Keys is not available on this version of Cassandra.");
		}

		BoundStatement statement = m_metaCluster.psServiceIndexListServiceKeys.boundStatementBuilder()
				.setString(0, service)
				.setConsistencyLevel(m_metaCluster.getReadConsistencyLevel())
				.build();

		ResultSet resultSet = m_metaCluster.execute(statement);
		resultSet.forEach(row -> {
			ret.add(resultSet.one().getString(0));
		});

		return ret;
	}

    @Override
	public Iterable<String> listKeys(String service, String serviceKey) throws DatastoreException
	{
		List<String> ret = new ArrayList<>();

		BoundStatement statement = m_metaCluster.psServiceIndexListKeys.boundStatementBuilder()
				.setString(0, service)
				.setString(1, serviceKey)
				.setConsistencyLevel(m_metaCluster.getReadConsistencyLevel())
				.build();

		ResultSet resultSet = m_metaCluster.execute(statement);
		resultSet.forEach(row -> {
			String key = resultSet.one().getString(0);
			if (key != null) {  // The last row for the primary key doesn't get deleted and has a null key and isExhausted still return false. So check for null
				ret.add(key);
			}
		});

		return ret;
	}

	@Override
	public Iterable<String> listKeys(String service, String serviceKey, String keyStartsWith) throws DatastoreException
	{
		String begin = keyStartsWith;
		String end = keyStartsWith + Character.MAX_VALUE;

		List<String> ret = new ArrayList<>();

		BoundStatement statement = m_metaCluster.psServiceIndexListKeysPrefix.boundStatementBuilder()
				.setString(0, service)
				.setString(1, serviceKey)
				.setString(2, begin)
				.setString(3, end)
				.setConsistencyLevel(m_metaCluster.getReadConsistencyLevel())
				.build();

		ResultSet resultSet = m_metaCluster.execute(statement);
		resultSet.forEach(row -> {
			String key = resultSet.one().getString(0);
			if (key != null) {  // The last row for the primary key doesn't get deleted and has a null key and isExhausted still return false. So check for null
				ret.add(key);
			}
		});

		return ret;
	}

	@Override
	public void deleteKey(String service, String serviceKey, String key)
			throws DatastoreException
	{
		BoundStatement statement = m_metaCluster.psServiceIndexDeleteKey.boundStatementBuilder()
				.setString(0, service)
				.setString(1, serviceKey)
				.setString(2, key)
				.setConsistencyLevel(m_metaCluster.getWriteConsistencyLevel())
				.build();

		m_metaCluster.execute(statement);

		// Update modification time
		statement = m_metaCluster.psServiceIndexInsertModifiedTime.boundStatementBuilder()
				.setString(0, service)
				.setString(1, serviceKey)
				.build();

		m_metaCluster.execute(statement);
	}

	@Override
	public Date getServiceKeyLastModifiedTime(String service, String serviceKey) throws DatastoreException
	{
		BoundStatement statement = m_metaCluster.psServiceIndexModificationTime.boundStatementBuilder()
				.setString(0, service)
				.setString(1, serviceKey)
				.build();

		ResultSet resultSet = m_metaCluster.execute(statement);
		Row row = resultSet.one();

		if (row != null)
			return new Date(Uuids.unixTimestamp(row.getUuid(0)));

		return new Date(0L);
	}

	@Override
	public void queryDatabase(DatastoreMetricQuery query, QueryCallback queryCallback) throws DatastoreException
	{
		cqlQueryWithRowKeys(query, queryCallback, getKeysForQueryIterator(query));
	}



	private class QueryListener implements AsyncResultCollector.PageProcessor
	{
		private final DataPointsRowKey m_rowKey;
		private final QueryCallback m_callback;
		private final Semaphore m_semaphore;  //Used to notify caller when last query is done
		private final QueryMonitor m_queryMonitor;
		private final RowSpec m_rowSpec;

		public QueryListener(DataPointsRowKey rowKey, QueryCallback callback, Semaphore querySemaphor, QueryMonitor queryMonitor, RowSpec rowSpec)
		{
			m_rowKey = rowKey;
			m_callback = callback;
			m_semaphore = querySemaphor;
			m_queryMonitor = queryMonitor;
			m_rowSpec = rowSpec;
		}

		@Override
		public void processPage(Iterable<Row> page)
		{
			try (QueryCallback.DataPointWriter dataPointWriter = m_callback.startDataPointSet(m_rowKey.getDataType(), m_rowKey.getTags()))
			{
				DataPointFactory dataPointFactory = null;
				dataPointFactory = m_kairosDataPointFactory.getFactoryForDataStoreType(m_rowKey.getDataType());

				for (Row row : page) {
					ByteBuffer bytes = row.getBytesUnsafe(0);

					int columnTime = bytes.getInt();

					ByteBuffer value = row.getBytesUnsafe(1);
					long timestamp = m_rowSpec.getColumnTimestamp(m_rowKey.getTimestamp(), columnTime);

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
			catch (Exception e)
			{
				logger.error("QueryListener failure on cluster "+m_rowKey.getClusterName(), e);
				m_queryMonitor.failQuery(e);
			}
			finally
			{
				m_semaphore.release();
			}
		}

		/*@Override
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
						long timestamp = m_rowSpec.getColumnTimestamp(m_rowKey.getTimestamp(), columnTime);

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
				logger.error("QueryListener failure on cluster "+m_rowKey.getClusterName(), e);
				m_queryMonitor.failQuery(e);
			}
			finally
			{
				m_semaphore.release();
			}
		}*/

		/*@Override
		public void onFailure(Throwable t)
		{
			logger.error("Async query failure on cluster "+m_rowKey.getClusterName(), t);
			m_queryMonitor.failQuery(t);
			m_semaphore.release();
		}*/


	}


	private void cqlQueryWithRowKeys(DatastoreMetricQuery query,
			QueryCallback queryCallback, Iterator<DataPointsRowKey> rowKeys) throws DatastoreException
	{
		AsyncResultCollector resultCollector = new AsyncResultCollector();
		int rowCount = 0;
		long queryStartTime = query.getStartTime();
		long queryEndTime = query.getEndTime();
		boolean useLimit = query.getLimit() != 0;
		QueryMonitor queryMonitor = new QueryMonitor(m_cassandraConfiguration.getQueryLimit(),
				m_cassandraConfiguration.getQueryTimeLimit());

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
			DataPointsRowKey rowKey = rowKeys.next();
			ClusterConnection cluster = m_clusterMap.get(rowKey.getClusterName());
			RowSpec rowSpec = cluster.getRowSpec();
			long rowWidth = rowSpec.getRowWidthInMillis();
			long tierRowTime = rowKey.getTimestamp();
			int startTime;
			int endTime;
			if (queryStartTime < tierRowTime)
				startTime = 0;
			else
				startTime = rowSpec.getColumnName(tierRowTime, queryStartTime);

			if (queryEndTime > (tierRowTime + rowWidth))
				endTime = rowSpec.getColumnName(tierRowTime, tierRowTime + rowWidth) +1;
			else
				endTime = rowSpec.getColumnName(tierRowTime, queryEndTime) +1; //add 1 so we get 0x1 for last bit

			ByteBuffer startBuffer = ByteBuffer.allocate(4);
			startBuffer.putInt(startTime);
			startBuffer.rewind();

			ByteBuffer endBuffer = ByteBuffer.allocate(4);
			endBuffer.putInt(endTime);
			endBuffer.rewind();

			BoundStatementBuilder boundStatement;
			if (useLimit)
			{
				if (query.getOrder() == Order.ASC)
					boundStatement = cluster.psDataPointsQueryAscLimit.boundStatementBuilder();
				else
					boundStatement = cluster.psDataPointsQueryDescLimit.boundStatementBuilder();
			}
			else
			{
				if (query.getOrder() == Order.ASC)
					boundStatement = cluster.psDataPointsQueryAsc.boundStatementBuilder();
				else
					boundStatement = cluster.psDataPointsQueryDesc.boundStatementBuilder();
			}

			boundStatement.setBytesUnsafe(0, DATA_POINTS_ROW_KEY_SERIALIZER.toByteBuffer(rowKey));
			boundStatement.setBytesUnsafe(1, startBuffer);
			boundStatement.setBytesUnsafe(2, endBuffer);

			if (useLimit)
				boundStatement.setInt(3, query.getLimit());

			boundStatement.setConsistencyLevel(cluster.getReadConsistencyLevel());

			try
			{
				querySemaphore.acquire();
			}
			catch (InterruptedException e)
			{
				queryMonitor.failQuery(e);
			}

			if (queryMonitor.keepRunning())
			{
				//use asncResultCollector and add this result to it.
				CompletionStage<AsyncResultSet> asyncResult = cluster.executeAsync(boundStatement.build());
				resultCollector.addResultSetPage(asyncResult, new QueryListener(rowKey, queryCallback, querySemaphore, queryMonitor, rowSpec), resultsExecutor);
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

		stats.rowKeyCount().put(rowCount);

		try
		{
			if (queryMonitor.getException() == null)
				//This is where it waits until all queries are done.  Need to use collector for this instead.
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

	private void deletePartialRow(DataPointsRowKey rowKey, long start, long end, ClusterConnection cluster) throws DatastoreException
	{
		RowSpec rowSpec = cluster.getRowSpec();
		if (cluster.psDataPointsDeleteRange != null)
		{
			BoundStatementBuilder statement = cluster.psDataPointsDeleteRange.boundStatementBuilder();
			statement.setBytesUnsafe(0, DATA_POINTS_ROW_KEY_SERIALIZER.toByteBuffer(rowKey));
			ByteBuffer b = ByteBuffer.allocate(4);
			b.putInt(rowSpec.getColumnName(rowKey.getTimestamp(), start));
			b.rewind();
			statement.setBytesUnsafe(1, b);

			b = ByteBuffer.allocate(4);
			b.putInt(rowSpec.getColumnName(rowKey.getTimestamp(), end));
			b.rewind();
			statement.setBytesUnsafe(2, b);

			statement.setConsistencyLevel(cluster.getReadConsistencyLevel());
			cluster.executeAsync(statement.build());
		}
		else
		{
			//note, with multiple old clusters this query could be done multiple times
			DatastoreMetricQuery deleteQuery = new QueryMetric(start, end, 0,
					rowKey.getMetricName());

			cqlQueryWithRowKeys(deleteQuery, new DeletingCallback(deleteQuery.getName(), rowSpec),
					Collections.singletonList(rowKey).iterator());
		}
	}


	@Override
	public void deleteDataPoints(DatastoreMetricQuery deleteQuery) throws DatastoreException
	{
		requireNonNull(deleteQuery);
		boolean clearCache = false;


		boolean deleteAll = deleteQuery.getStartTime() == Long.MIN_VALUE && deleteQuery.getEndTime() == Long.MAX_VALUE;

		Iterator<DataPointsRowKey> rowKeyIterator = getKeysForQueryIterator(deleteQuery);

		while (rowKeyIterator.hasNext())
		{
			DataPointsRowKey rowKey = rowKeyIterator.next();
			ClusterConnection cluster = m_clusterMap.get(rowKey.getClusterName());
			long rowWidth = cluster.getRowSpec().getRowWidthInMillis();

			//System.out.println("Deleting from row "+rowKey);
			long rowKeyTimestamp = rowKey.getTimestamp();
			if (deleteQuery.getStartTime() <= rowKeyTimestamp && (deleteQuery.getEndTime() >= rowKeyTimestamp + rowWidth - 1))
			{

				//System.out.println("Delete entire row");
				Statement statement = cluster.psDataPointsDeleteRow.boundStatementBuilder()
						.setBytesUnsafe(0, DATA_POINTS_ROW_KEY_SERIALIZER.toByteBuffer(rowKey))
						.setConsistencyLevel(cluster.getReadConsistencyLevel())
						.build();
				cluster.execute(statement);

				//Delete from old row keys
				statement = cluster.psRowKeyIndexDelete.boundStatementBuilder()
						.setBytesUnsafe(0, serializeString(rowKey.getMetricName()))
						.setBytesUnsafe(1, DATA_POINTS_ROW_KEY_SERIALIZER.toByteBuffer(rowKey))
						.setConsistencyLevel(cluster.getReadConsistencyLevel())
						.build();
				cluster.execute(statement);

				RowKeyLookup rowKeyLookup = cluster.getRowKeyLookupForMetric(rowKey.getMetricName());
				for (Statement rowKeyDeleteStmt : rowKeyLookup.createDeleteStatements(rowKey))
				{
					rowKeyDeleteStmt.setConsistencyLevel(cluster.getReadConsistencyLevel());
					cluster.execute(rowKeyDeleteStmt);
				}

				//Should only remove if the entire time window goes away and no tags are specified in query
				//todo if we allow deletes for specific types this needs to change
				if (deleteQuery.getTags().isEmpty())
				{
					statement = cluster.psRowKeyTimeDelete.boundStatementBuilder()
							.setString(0, rowKey.getMetricName())
							.setString(1, DATA_POINTS_TABLE_NAME)
							.setInstant(2, Instant.ofEpochMilli(rowKey.getTimestamp()))
							.setConsistencyLevel(cluster.getReadConsistencyLevel())
							.build();
					cluster.execute(statement);
				}

				clearCache = true;
			}
			else if (deleteQuery.getStartTime() <= rowKeyTimestamp)
			{
				//System.out.println("Delete first of row");
				//Delete first portion of row
				//deletePartialRow(rowKey, 0, getColumnName(rowKeyTimestamp, deleteQuery.getEndTime()));
				deletePartialRow(rowKey, rowKeyTimestamp, deleteQuery.getEndTime(), cluster);
			}
			else if (deleteQuery.getEndTime() >= rowKeyTimestamp + rowWidth -1)
			{
				//System.out.println("Delete last of row");
				//Delete last portion of row
				//deletePartialRow(rowKey, getColumnName(rowKeyTimestamp, deleteQuery.getStartTime()),
				//		getColumnName(rowKeyTimestamp, rowKeyTimestamp + ROW_WIDTH - 1));
				deletePartialRow(rowKey, deleteQuery.getStartTime(),
						rowKeyTimestamp + rowWidth - 1, cluster);
			}
			else
			{
				//System.out.println("Delete within a row");
				//Delete within a row
				/*deletePartialRow(rowKey, getColumnName(rowKeyTimestamp, deleteQuery.getStartTime()),
						getColumnName(rowKeyTimestamp, deleteQuery.getEndTime()));*/
				deletePartialRow(rowKey, deleteQuery.getStartTime(),
						deleteQuery.getEndTime(), cluster);
			}
		}

		// If index is gone, delete metric name from Strings column family
		if (deleteAll)
		{
			iterateClusters((cluster) ->
					{
						BoundStatement statement = cluster.psRowKeyIndexDeleteRow.boundStatementBuilder()
								.setBytesUnsafe(0, serializeString(deleteQuery.getName()))
								.setConsistencyLevel(cluster.getReadConsistencyLevel())
								.build();
						cluster.executeAsync(statement);

						//Delete from string index
						statement = (cluster.psStringIndexDelete).boundStatementBuilder()
								.setBytesUnsafe(0, serializeString(ROW_KEY_METRIC_NAMES))
								.setBytesUnsafe(1, serializeString(deleteQuery.getName()))
								.setConsistencyLevel(cluster.getReadConsistencyLevel())
								.build();
						cluster.executeAsync(statement);
						return null;
					});

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

		if (ret == null && query.isExplicitTags())
		{
			//todo I should really finish this
		}

		//Default to query index if no plugin was provided
		if (ret == null)
		{
			List<Iterator<DataPointsRowKey>> retList = new ArrayList<>();

			//todo use Iterable.concat to query multiple metrics at the same time.
			//each filtered iterator will be combined into one and returned.
			//one issue is that the queries are done in the constructor
			//would like to do them lazily but would have to throw an exception through
			//hasNext call, ick
			if (m_writeCluster.containRange(query.getStartTime(), query.getEndTime()))
			{
				retList.add(m_rowKeyFilterFactory.create(m_writeCluster, query.getName(), query.getStartTime(),
						query.getEndTime(), query.getTags()));
			}

			for (ClusterConnection cluster : m_readClusters)
			{
				if (cluster.containRange(query.getStartTime(), query.getEndTime()))
				{
					retList.add(m_rowKeyFilterFactory.create(cluster, query.getName(), query.getStartTime(),
							query.getEndTime(), query.getTags()));
				}
			}

			ret = Iterators.concat(retList.iterator());
		}

		return (ret);
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


	public static boolean isLongValue(int columnName)
	{
		return ((columnName & 0x1) == LONG_FLAG);
	}


	private class DeletingCallback implements QueryCallback
	{
		private final String m_metricName;
		RowSpec m_rowSpec;

		public DeletingCallback(String metricName, RowSpec rowSpec)
		{
			m_metricName = metricName;
			m_rowSpec = rowSpec;
		}


		@Override
		public DataPointWriter startDataPointSet(String dataType, SortedMap<String, String> tags) throws IOException
		{
			return new DeleteDatePointWriter(dataType, tags);
		}

		private class DeleteDatePointWriter implements DataPointWriter
		{
			private final String m_dataType;
			private final SortedMap<String, String> m_tags;
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
							m_metricName, m_tags, dataPoints, s_dontCareCallBack, m_rowSpec);

					m_congestionExecutor.submit(deleteBatchHandler);
				}
			}

			@Override
			public void close() throws IOException
			{
				if (m_dataPoints.size() != 0)
				{
					DeleteBatchHandler deleteBatchHandler = m_deleteBatchHandlerFactory.create(
							m_metricName, m_tags, m_dataPoints, s_dontCareCallBack, m_rowSpec);

					m_congestionExecutor.submit(deleteBatchHandler);
				}
			}
		}
	}

	private void printHosts(Iterator<Node> nodeIterator)
	{
		StringBuilder sb = new StringBuilder();

		while (nodeIterator.hasNext())
		{
			Node node = nodeIterator.next();
			sb.append(node.getEndPoint()).append(":").append(node.getDatacenter());
			if (nodeIterator.hasNext())
				sb.append(", ");
		}

		logger.info("Hosts: "+sb.toString());
	}

	private static final IDontCareCallBack s_dontCareCallBack = new IDontCareCallBack();

	private static class IDontCareCallBack implements EventCompletionCallBack
	{
		@Override
		public void complete() {} //Dont care
	}

}
