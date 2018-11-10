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

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import org.hamcrest.CoreMatchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.TestDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datastore.CachedSearchResult;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.DataPointRow;
import org.kairosdb.core.datastore.DatastoreMetricQuery;
import org.kairosdb.core.datastore.DatastoreQuery;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.datastore.QueryQueuingManager;
import org.kairosdb.core.datastore.ServiceKeyValue;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.queue.EventCompletionCallBack;
import org.kairosdb.core.queue.MemoryQueueProcessor;
import org.kairosdb.datastore.DatastoreMetricQueryImpl;
import org.kairosdb.datastore.DatastoreTestHelper;
import org.kairosdb.events.DataPointEvent;
import org.kairosdb.util.IngestExecutorService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Executors;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


public class CassandraDatastoreTest extends DatastoreTestHelper
{
	private static final String ROW_KEY_TEST_METRIC = "row_key_test_metric";
	private static final String ROW_KEY_BIG_METRIC = "row_key_big_metric";

	private static final int MAX_ROW_READ_SIZE = 1024;
	private static final int OVERFLOW_SIZE = MAX_ROW_READ_SIZE * 2 + 10;

	private static KairosDataPointFactory dataPointFactory = new TestDataPointFactory();
	private static Random random = new Random();
	private static CassandraDatastore s_datastore;
	private static long s_dataPointTime;
	private static final HashMultimap<String,String> EMPTY_MAP = HashMultimap.create();
	private static Schema m_schema;

	private static void putDataPoints(DataPointSet dps) throws DatastoreException
	{
		for (DataPoint dataPoint : dps.getDataPoints())
		{
			s_eventBus.createPublisher(DataPointEvent.class).post(new DataPointEvent(dps.getName(), dps.getTags(), dataPoint, 0));
		}
	}

	private static void loadCassandraData() throws DatastoreException
	{
		s_dataPointTime = System.currentTimeMillis();

		metricNames.add(ROW_KEY_TEST_METRIC);

		DataPointSet dpSet = new DataPointSet(ROW_KEY_TEST_METRIC);
		dpSet.addTag("host", "A");
		dpSet.addTag("client", "foo");

		dpSet.addDataPoint(new LongDataPoint(s_dataPointTime, 42));

		putDataPoints(dpSet);


		dpSet = new DataPointSet(ROW_KEY_TEST_METRIC);
		dpSet.addTag("host", "B");
		dpSet.addTag("client", "foo");

		dpSet.addDataPoint(new LongDataPoint(s_dataPointTime, 42));

		putDataPoints(dpSet);


		dpSet = new DataPointSet(ROW_KEY_TEST_METRIC);
		dpSet.addTag("host", "C");
		dpSet.addTag("client", "bar");

		dpSet.addDataPoint(new LongDataPoint(s_dataPointTime, 42));

		putDataPoints(dpSet);


		dpSet = new DataPointSet(ROW_KEY_TEST_METRIC);
		dpSet.addTag("host", "D");
		dpSet.addTag("client", "bar");

		dpSet.addDataPoint(new LongDataPoint(s_dataPointTime, 42));

		putDataPoints(dpSet);


		// Add a row of data that is larger than MAX_ROW_READ_SIZE
		dpSet = new DataPointSet(ROW_KEY_BIG_METRIC);
		dpSet.addTag("host", "E");

		for (int i = OVERFLOW_SIZE; i > 0; i--)
		{
			dpSet.addDataPoint(new LongDataPoint(s_dataPointTime - (long) i, 42));
		}

		putDataPoints(dpSet);


		// NOTE: This data will be deleted by delete tests. Do not expect it to be there.
		metricNames.add("MetricToDelete");
		dpSet = new DataPointSet("MetricToDelete");
		dpSet.addTag("host", "A");
		dpSet.addTag("client", "bar");

		long rowKeyTime = CassandraDatastore.calculateRowTime(s_dataPointTime);
		dpSet.addDataPoint(new LongDataPoint(rowKeyTime, 13));
		dpSet.addDataPoint(new LongDataPoint(rowKeyTime + 1000, 14));
		dpSet.addDataPoint(new LongDataPoint(rowKeyTime + 2000, 15));
		dpSet.addDataPoint(new LongDataPoint(rowKeyTime + 3000, 16));

		putDataPoints(dpSet);

		// NOTE: This data will be deleted by delete tests. Do not expect it to be there.
		metricNames.add("OtherMetricToDelete");
		dpSet = new DataPointSet("OtherMetricToDelete");
		dpSet.addTag("host", "B");
		dpSet.addTag("client", "bar");

		dpSet.addDataPoint(new LongDataPoint(rowKeyTime, 13));
		dpSet.addDataPoint(new LongDataPoint(rowKeyTime + CassandraDatastore.ROW_WIDTH, 14));
		dpSet.addDataPoint(new LongDataPoint(rowKeyTime + (2 * CassandraDatastore.ROW_WIDTH), 15));
		dpSet.addDataPoint(new LongDataPoint(rowKeyTime + (3 * CassandraDatastore.ROW_WIDTH), 16));

		putDataPoints(dpSet);

		// NOTE: This data will be deleted by delete tests. Do not expect it to be there.
		metricNames.add("MetricToPartiallyDelete");
		dpSet = new DataPointSet("MetricToPartiallyDelete");
		dpSet.addTag("host", "B");
		dpSet.addTag("client", "bar");

		dpSet.addDataPoint(new LongDataPoint(rowKeyTime, 13));
		dpSet.addDataPoint(new LongDataPoint(rowKeyTime + CassandraDatastore.ROW_WIDTH, 14));
		dpSet.addDataPoint(new LongDataPoint(rowKeyTime + (2 * CassandraDatastore.ROW_WIDTH), 15));
		dpSet.addDataPoint(new LongDataPoint(rowKeyTime + (3 * CassandraDatastore.ROW_WIDTH), 16));

		putDataPoints(dpSet);

		// NOTE: This data will be deleted by delete tests. Do not expect it to be there.
		metricNames.add("YetAnotherMetricToDelete");
		dpSet = new DataPointSet("YetAnotherMetricToDelete");
		dpSet.addTag("host", "A");
		dpSet.addTag("client", "bar");

		dpSet.addDataPoint(new LongDataPoint(rowKeyTime, 13));
		dpSet.addDataPoint(new LongDataPoint(rowKeyTime + 1000, 14));
		dpSet.addDataPoint(new LongDataPoint(rowKeyTime + 2000, 15));
		dpSet.addDataPoint(new LongDataPoint(rowKeyTime + 3000, 16));

		putDataPoints(dpSet);

		//These metrics are for deleting without using a range delete.
		metricNames.add("MetricToDelete2");
		dpSet = new DataPointSet("MetricToDelete2");
		dpSet.addTag("host", "A");
		dpSet.addTag("client", "bar");

		rowKeyTime = CassandraDatastore.calculateRowTime(s_dataPointTime);
		dpSet.addDataPoint(new LongDataPoint(rowKeyTime, 13));
		dpSet.addDataPoint(new LongDataPoint(rowKeyTime + 1000, 14));
		dpSet.addDataPoint(new LongDataPoint(rowKeyTime + 2000, 15));
		dpSet.addDataPoint(new LongDataPoint(rowKeyTime + 3000, 16));

		putDataPoints(dpSet);

		// NOTE: This data will be deleted by delete tests. Do not expect it to be there.
		metricNames.add("OtherMetricToDelete2");
		dpSet = new DataPointSet("OtherMetricToDelete2");
		dpSet.addTag("host", "B");
		dpSet.addTag("client", "bar");

		dpSet.addDataPoint(new LongDataPoint(rowKeyTime, 13));
		dpSet.addDataPoint(new LongDataPoint(rowKeyTime + CassandraDatastore.ROW_WIDTH, 14));
		dpSet.addDataPoint(new LongDataPoint(rowKeyTime + (2 * CassandraDatastore.ROW_WIDTH), 15));
		dpSet.addDataPoint(new LongDataPoint(rowKeyTime + (3 * CassandraDatastore.ROW_WIDTH), 16));

		putDataPoints(dpSet);

		// NOTE: This data will be deleted by delete tests. Do not expect it to be there.
		metricNames.add("MetricToPartiallyDelete2");
		dpSet = new DataPointSet("MetricToPartiallyDelete2");
		dpSet.addTag("host", "B");
		dpSet.addTag("client", "bar");

		dpSet.addDataPoint(new LongDataPoint(rowKeyTime, 13));
		dpSet.addDataPoint(new LongDataPoint(rowKeyTime + CassandraDatastore.ROW_WIDTH, 14));
		dpSet.addDataPoint(new LongDataPoint(rowKeyTime + (2 * CassandraDatastore.ROW_WIDTH), 15));
		dpSet.addDataPoint(new LongDataPoint(rowKeyTime + (3 * CassandraDatastore.ROW_WIDTH), 16));

		putDataPoints(dpSet);

		// NOTE: This data will be deleted by delete tests. Do not expect it to be there.
		metricNames.add("YetAnotherMetricToDelete2");
		dpSet = new DataPointSet("YetAnotherMetricToDelete2");
		dpSet.addTag("host", "A");
		dpSet.addTag("client", "bar");

		dpSet.addDataPoint(new LongDataPoint(rowKeyTime, 13));
		dpSet.addDataPoint(new LongDataPoint(rowKeyTime + 1000, 14));
		dpSet.addDataPoint(new LongDataPoint(rowKeyTime + 2000, 15));
		dpSet.addDataPoint(new LongDataPoint(rowKeyTime + 3000, 16));

		putDataPoints(dpSet);
	}

	@BeforeClass
	public static void setupDatastore() throws InterruptedException, DatastoreException
	{
		String cassandraHost = "localhost";
		if (System.getenv("CASSANDRA_HOST") != null)
			cassandraHost = System.getenv("CASSANDRA_HOST");

		CassandraConfiguration configuration = new CassandraConfiguration("kairosdb_test");
		configuration.setHostList(cassandraHost);
		CassandraClient client = new CassandraClientImpl(configuration);
		((CassandraClientImpl) client).init();
		m_schema = new Schema(client, true);
		Session session = m_schema.getSession();
		BatchStats batchStats = new BatchStats();
		DataCache<DataPointsRowKey> rowKeyCache = new DataCache<>(1024);
		DataCache<String> metricNameCache = new DataCache<>(1024);

		CassandraModule.CQLBatchFactory cqlBatchFactory = new CassandraModule.CQLBatchFactory()
		{
			@Override
			public CQLBatch create()
			{
				return new CQLBatch(ConsistencyLevel.QUORUM, session, m_schema,
						batchStats, client.getWriteLoadBalancingPolicy());
			}
		};

		s_datastore = new CassandraDatastore(
				client,
				configuration,
				m_schema,
				session,
				dataPointFactory,
				new MemoryQueueProcessor(Executors.newSingleThreadExecutor(), 1000, 10000, 10, 500),
				new IngestExecutorService(s_eventBus, 1),
				new CassandraModule.BatchHandlerFactory()
				{
					@Override
					public BatchHandler create(List<DataPointEvent> events, EventCompletionCallBack callBack, boolean fullBatch)
					{
						return new BatchHandler(events, callBack,
								configuration, rowKeyCache, metricNameCache,
								s_eventBus, cqlBatchFactory);
					}
				},
				new CassandraModule.DeleteBatchHandlerFactory()
				{
					@Override
					public DeleteBatchHandler create(String metricName, SortedMap<String,
							String> tags, List<DataPoint> dataPoints, EventCompletionCallBack callBack)
					{
						return new DeleteBatchHandler(metricName, tags, dataPoints,
								callBack, cqlBatchFactory);
					}
				});

		DatastoreTestHelper.s_datastore = new KairosDatastore(s_datastore,
				new QueryQueuingManager(1, "hostname"),
				dataPointFactory, false);

		s_eventBus.register(s_datastore);

		loadCassandraData();
		loadData();
		Thread.sleep(2000);

	}

	@AfterClass
	public static void closeDatastore() throws InterruptedException, IOException, DatastoreException
	{
		for (String metricName : metricNames)
		{
			deleteMetric(metricName);
		}

		s_datastore.close();
	}

	private static List<DataPointsRowKey> readIterator(Iterator<DataPointsRowKey> it)
	{
		List<DataPointsRowKey> ret = new ArrayList<>();
		while (it.hasNext())
			ret.add(it.next());

		return (ret);
	}

	private static void deleteMetric(String metricName) throws IOException, DatastoreException
	{
		DatastoreMetricQueryImpl query = new DatastoreMetricQueryImpl(metricName, EMPTY_MAP, 0L, Long.MAX_VALUE);
		s_datastore.deleteDataPoints(query);
	}

	@Test
	public void test_getKeysForQuery() throws DatastoreException
	{
		DatastoreMetricQuery query = new DatastoreMetricQueryImpl(ROW_KEY_TEST_METRIC,
				HashMultimap.create(), s_dataPointTime, s_dataPointTime);

		List<DataPointsRowKey> keys = readIterator(s_datastore.getKeysForQueryIterator(query));

		assertEquals(4, keys.size());
	}

	@Test
	public void test_getKeysForQuery_withFilter() throws DatastoreException
	{
		SetMultimap<String, String> tagFilter = HashMultimap.create();
		tagFilter.put("client", "bar");

		DatastoreMetricQuery query = new DatastoreMetricQueryImpl(ROW_KEY_TEST_METRIC,
				tagFilter, s_dataPointTime, s_dataPointTime);

		List<DataPointsRowKey> keys = readIterator(s_datastore.getKeysForQueryIterator(query));

		assertEquals(2, keys.size());
	}

	@Test
	public void test_rowLargerThanMaxReadSize() throws DatastoreException
	{
		Map<String, String> tagFilter = new HashMap<>();
		tagFilter.put("host", "E");

		QueryMetric query = new QueryMetric(s_dataPointTime - OVERFLOW_SIZE, 0, ROW_KEY_BIG_METRIC);
		query.setEndTime(s_dataPointTime);
		query.setTags(tagFilter);

		DatastoreQuery dq = DatastoreTestHelper.s_datastore.createQuery(query);
		List<DataPointGroup> results = dq.execute();

		DataPointGroup dataPointGroup = results.get(0);
		int counter = 0;
		int total = 0;
		while (dataPointGroup.hasNext())
		{
			DataPoint dp = dataPointGroup.next();
			total += dp.getLongValue();
			counter++;
		}

		dataPointGroup.close();
		assertThat(total, equalTo(counter * 42));
		assertEquals(OVERFLOW_SIZE, counter);
		dq.close();
	}

	@Test (expected = NullPointerException.class)
	public void test_deleteDataPoints_nullQuery_Invalid() throws IOException, DatastoreException
	{
		s_datastore.deleteDataPoints(null);
	}


	@Test
	public void test_deleteDataPoints_DeleteEntireRow() throws IOException, DatastoreException, InterruptedException
	{
		String metricToDelete = "MetricToDelete";
		DatastoreMetricQuery query = new DatastoreMetricQueryImpl(metricToDelete, EMPTY_MAP, Long.MIN_VALUE, Long.MAX_VALUE);

		CachedSearchResult res = createCache(metricToDelete);
		s_datastore.queryDatabase(query, res);
		List<DataPointRow> rows = res.getRows();

		assertThat(rows.size(), equalTo(1));

		s_datastore.deleteDataPoints(query);
		Thread.sleep(2000);

		// Verify that all data points are gone
		res = createCache(metricToDelete);
		s_datastore.queryDatabase(query, res);
		rows = res.getRows();
		assertThat(rows.size(), equalTo(0));

		// Verify that the index key is gone
		List<DataPointsRowKey> indexRowKeys = readIterator(s_datastore.getKeysForQueryIterator(query));
		assertThat(indexRowKeys.size(), equalTo(0));

		// Verify that the metric name is gone from the Strings column family
		assertThat(s_datastore.getMetricNames(null), not(hasItem(metricToDelete)));
	}

	@Test
	public void test_deleteDataPoints_DeleteColumnsSpanningRows() throws IOException, DatastoreException, InterruptedException
	{
		String metricToDelete = "OtherMetricToDelete";
		DatastoreMetricQuery query = new DatastoreMetricQueryImpl(metricToDelete, EMPTY_MAP, Long.MIN_VALUE, Long.MAX_VALUE);

		CachedSearchResult res = createCache(metricToDelete);
		s_datastore.queryDatabase(query, res);
		List<DataPointRow> rows = res.getRows();
		assertThat(rows.size(), equalTo(4));

		s_datastore.deleteDataPoints(query);
		Thread.sleep(2000);

		res = createCache(metricToDelete);
		s_datastore.queryDatabase(query, res);
		rows = res.getRows();
		assertThat(rows.size(), equalTo(0));

		// Verify that the index key is gone
		DatastoreMetricQueryImpl queryEverything = new DatastoreMetricQueryImpl(metricToDelete, EMPTY_MAP, 0L, Long.MAX_VALUE);
		List<DataPointsRowKey> indexRowKeys = readIterator(s_datastore.getKeysForQueryIterator(queryEverything));
		assertThat(indexRowKeys.size(), equalTo(0));

		// Verify that the metric name is gone from the Strings column family
		assertThat(s_datastore.getMetricNames(null), not(hasItem(metricToDelete)));
	}

	@Test
	public void test_deleteDataPoints_DeleteColumnsSpanningRows_rowsLeft() throws IOException, DatastoreException, InterruptedException
	{
		long rowKeyTime = CassandraDatastore.calculateRowTime(s_dataPointTime);
		String metricToDelete = "MetricToPartiallyDelete";
		DatastoreMetricQuery query = new DatastoreMetricQueryImpl(metricToDelete, EMPTY_MAP, 0L, Long.MAX_VALUE);

		CachedSearchResult res = createCache(metricToDelete);
		s_datastore.queryDatabase(query, res);
		List<DataPointRow> rows = res.getRows();
		assertThat(rows.size(), equalTo(4));

		DatastoreMetricQuery deleteQuery = new DatastoreMetricQueryImpl(metricToDelete, EMPTY_MAP, 0L,
				rowKeyTime + (3 * CassandraDatastore.ROW_WIDTH - 1));
		s_datastore.deleteDataPoints(deleteQuery);
		Thread.sleep(2000);

		res = createCache(metricToDelete);
		s_datastore.queryDatabase(query, res);
		rows = res.getRows();
		assertThat(rows.size(), equalTo(1));

		// Verify that the index key is gone
		DatastoreMetricQueryImpl queryEverything = new DatastoreMetricQueryImpl(metricToDelete, EMPTY_MAP, 0L, Long.MAX_VALUE);
		List<DataPointsRowKey> indexRowKeys = readIterator(s_datastore.getKeysForQueryIterator(queryEverything));
		assertThat(indexRowKeys.size(), equalTo(1));

		// Verify that the metric name still exists in the Strings column family
		assertThat(s_datastore.getMetricNames(null), hasItem(metricToDelete));
	}

	@Test
	public void test_deleteDataPoints_DeleteColumnWithinRow() throws IOException, DatastoreException, InterruptedException
	{
		long rowKeyTime = CassandraDatastore.calculateRowTime(s_dataPointTime);
		String metricToDelete = "YetAnotherMetricToDelete";
		DatastoreMetricQuery query = new DatastoreMetricQueryImpl(metricToDelete,
				EMPTY_MAP, rowKeyTime, rowKeyTime + 2000);

		CachedSearchResult res = createCache(metricToDelete);
		s_datastore.queryDatabase(query, res);
		List<DataPointRow> rows = res.getRows();
		assertThat(rows.size(), equalTo(1));

		s_datastore.deleteDataPoints(query);
		Thread.sleep(2000);

		res = createCache(metricToDelete);
		s_datastore.queryDatabase(query, res);
		rows = res.getRows();

		assertThat(rows.size(), equalTo(0));

		// Verify that the index key is still there
		DatastoreMetricQueryImpl queryEverything = new DatastoreMetricQueryImpl(metricToDelete, EMPTY_MAP, 0L, Long.MAX_VALUE);
		List<DataPointsRowKey> indexRowKeys = readIterator(s_datastore.getKeysForQueryIterator(queryEverything));
		assertThat(indexRowKeys.size(), equalTo(1));

		// Verify that the metric name still exists in the Strings column family
		assertThat(s_datastore.getMetricNames(null), hasItem(metricToDelete));
	}

	@Test
	public void test_deleteDataPoints_DeleteEntireRow2() throws IOException, DatastoreException, InterruptedException
	{
		m_schema.psDataPointsDeleteRange = null;
		String metricToDelete = "MetricToDelete2";
		DatastoreMetricQuery query = new DatastoreMetricQueryImpl(metricToDelete, EMPTY_MAP, Long.MIN_VALUE, Long.MAX_VALUE);

		CachedSearchResult res = createCache(metricToDelete);
		s_datastore.queryDatabase(query, res);
		List<DataPointRow> rows = res.getRows();

		assertThat(rows.size(), equalTo(1));

		s_datastore.deleteDataPoints(query);
		Thread.sleep(2000);

		// Verify that all data points are gone
		res = createCache(metricToDelete);
		s_datastore.queryDatabase(query, res);
		rows = res.getRows();
		assertThat(rows.size(), equalTo(0));

		// Verify that the index key is gone
		List<DataPointsRowKey> indexRowKeys = readIterator(s_datastore.getKeysForQueryIterator(query));
		assertThat(indexRowKeys.size(), equalTo(0));

		// Verify that the metric name is gone from the Strings column family
		assertThat(s_datastore.getMetricNames(null), not(hasItem(metricToDelete)));
	}

	@Test
	public void test_deleteDataPoints_DeleteColumnsSpanningRows2() throws IOException, DatastoreException, InterruptedException
	{
		m_schema.psDataPointsDeleteRange = null;
		String metricToDelete = "OtherMetricToDelete2";
		DatastoreMetricQuery query = new DatastoreMetricQueryImpl(metricToDelete, EMPTY_MAP, Long.MIN_VALUE, Long.MAX_VALUE);

		CachedSearchResult res = createCache(metricToDelete);
		s_datastore.queryDatabase(query, res);
		List<DataPointRow> rows = res.getRows();
		assertThat(rows.size(), equalTo(4));

		s_datastore.deleteDataPoints(query);
		Thread.sleep(2000);

		res = createCache(metricToDelete);
		s_datastore.queryDatabase(query, res);
		rows = res.getRows();
		assertThat(rows.size(), equalTo(0));

		// Verify that the index key is gone
		DatastoreMetricQueryImpl queryEverything = new DatastoreMetricQueryImpl(metricToDelete, EMPTY_MAP, 0L, Long.MAX_VALUE);
		List<DataPointsRowKey> indexRowKeys = readIterator(s_datastore.getKeysForQueryIterator(queryEverything));
		assertThat(indexRowKeys.size(), equalTo(0));

		// Verify that the metric name is gone from the Strings column family
		assertThat(s_datastore.getMetricNames(null), not(hasItem(metricToDelete)));
	}

	@Test
	public void test_deleteDataPoints_DeleteColumnsSpanningRows_rowsLeft2() throws IOException, DatastoreException, InterruptedException
	{
		m_schema.psDataPointsDeleteRange = null;
		long rowKeyTime = CassandraDatastore.calculateRowTime(s_dataPointTime);
		String metricToDelete = "MetricToPartiallyDelete2";
		DatastoreMetricQuery query = new DatastoreMetricQueryImpl(metricToDelete, EMPTY_MAP, 0L, Long.MAX_VALUE);

		CachedSearchResult res = createCache(metricToDelete);
		s_datastore.queryDatabase(query, res);
		List<DataPointRow> rows = res.getRows();
		assertThat(rows.size(), equalTo(4));

		DatastoreMetricQuery deleteQuery = new DatastoreMetricQueryImpl(metricToDelete, EMPTY_MAP, 0L,
				rowKeyTime + (3 * CassandraDatastore.ROW_WIDTH - 1));
		s_datastore.deleteDataPoints(deleteQuery);
		Thread.sleep(2000);

		res = createCache(metricToDelete);
		s_datastore.queryDatabase(query, res);
		rows = res.getRows();
		assertThat(rows.size(), equalTo(1));

		// Verify that the index key is gone
		DatastoreMetricQueryImpl queryEverything = new DatastoreMetricQueryImpl(metricToDelete, EMPTY_MAP, 0L, Long.MAX_VALUE);
		List<DataPointsRowKey> indexRowKeys = readIterator(s_datastore.getKeysForQueryIterator(queryEverything));
		assertThat(indexRowKeys.size(), equalTo(1));

		// Verify that the metric name still exists in the Strings column family
		assertThat(s_datastore.getMetricNames(null), hasItem(metricToDelete));
	}

	@Test
	public void test_deleteDataPoints_DeleteColumnWithinRow2() throws IOException, DatastoreException, InterruptedException
	{
		m_schema.psDataPointsDeleteRange = null;
		long rowKeyTime = CassandraDatastore.calculateRowTime(s_dataPointTime);
		String metricToDelete = "YetAnotherMetricToDelete2";
		DatastoreMetricQuery query = new DatastoreMetricQueryImpl(metricToDelete,
				EMPTY_MAP, rowKeyTime, rowKeyTime + 2000);

		CachedSearchResult res = createCache(metricToDelete);
		s_datastore.queryDatabase(query, res);
		List<DataPointRow> rows = res.getRows();
		assertThat(rows.size(), equalTo(1));

		s_datastore.deleteDataPoints(query);
		Thread.sleep(2000);

		res = createCache(metricToDelete);
		s_datastore.queryDatabase(query, res);
		rows = res.getRows();

		assertThat(rows.size(), equalTo(0));

		// Verify that the index key is still there
		DatastoreMetricQueryImpl queryEverything = new DatastoreMetricQueryImpl(metricToDelete, EMPTY_MAP, 0L, Long.MAX_VALUE);
		List<DataPointsRowKey> indexRowKeys = readIterator(s_datastore.getKeysForQueryIterator(queryEverything));
		assertThat(indexRowKeys.size(), equalTo(1));

		// Verify that the metric name still exists in the Strings column family
		assertThat(s_datastore.getMetricNames(null), hasItem(metricToDelete));
	}

	/**
	 This is here because hbase throws an exception in this case
	 @throws DatastoreException bla
	 */
	@Test
	public void test_queryDatabase_noMetric() throws DatastoreException
	{

		Map<String, String> tags = new TreeMap<>();
		QueryMetric query = new QueryMetric(500, 0, "metric_not_there");
		query.setEndTime(3000);

		query.setTags(tags);

		DatastoreQuery dq = DatastoreTestHelper.s_datastore.createQuery(query);

		List<DataPointGroup> results = dq.execute();

		assertThat(results.size(), CoreMatchers.equalTo(1));
		DataPointGroup dpg = results.get(0);
		assertThat(dpg.getName(), is("metric_not_there"));
		assertFalse(dpg.hasNext());

		dq.close();
	}

	@Test
	public void test_TimestampsCloseToZero() throws DatastoreException
	{
		DataPointSet set = new DataPointSet("testMetric");
		set.addDataPoint(new LongDataPoint(1, 1L));
		set.addDataPoint(new LongDataPoint(2, 2L));
		set.addDataPoint(new LongDataPoint(0, 3L));
		set.addDataPoint(new LongDataPoint(3, 4L));
		set.addDataPoint(new LongDataPoint(4, 5L));
		set.addDataPoint(new LongDataPoint(5, 6L));
		putDataPoints(set);
	}

	private static CachedSearchResult createCache(String metricName) throws IOException
	{
		String tempFile = System.getProperty("java.io.tmpdir");
		return CachedSearchResult.createCachedSearchResult(metricName,
				tempFile + "/" + random.nextLong(), dataPointFactory, false);
	}

	@Test
	public void test_setTTL() throws DatastoreException, InterruptedException
	{
		DataPointSet set = new DataPointSet("ttlMetric");
		set.addTag("tag", "value");
		set.addDataPoint(new LongDataPoint(1, 1L));
		set.addDataPoint(new LongDataPoint(2, 2L));
		set.addDataPoint(new LongDataPoint(0, 3L));
		set.addDataPoint(new LongDataPoint(3, 4L));
		set.addDataPoint(new LongDataPoint(4, 5L));
		set.addDataPoint(new LongDataPoint(5, 6L));
		putDataPoints(set);

		s_eventBus.createPublisher(DataPointEvent.class).post(new DataPointEvent("ttlMetric", set.getTags(),
				new LongDataPoint(50, 7L), 1));

		Thread.sleep(2000);
		Map<String, String> tags = new TreeMap<>();
		QueryMetric query = new QueryMetric(0, 500, 0, "ttlMetric");

		query.setTags(tags);

		DatastoreQuery dq = DatastoreTestHelper.s_datastore.createQuery(query);

		List<DataPointGroup> results = dq.execute();
		try
		{
			assertThat(results.size(), CoreMatchers.equalTo(1));
			DataPointGroup dpg = results.get(0);
			assertThat(dpg.getName(), is("ttlMetric"));
			assertThat(dq.getSampleSize(), CoreMatchers.equalTo(6));
		}
		finally
		{
			dq.close();
		}
	}

	@Test
	public void test_serviceKeyStore_singleService()
			throws DatastoreException
	{
		s_datastore.setValue("Service", "ServiceKey", "key1", "value1");
		s_datastore.setValue("Service", "ServiceKey", "key2", "value2");
		s_datastore.setValue("Service", "ServiceKey", "foo", "value3");

		// Test setValue and getValue
		assertServiceKeyValue("Service", "ServiceKey", "key1", "value1");
		assertServiceKeyValue("Service", "ServiceKey", "key2", "value2");
		assertServiceKeyValue("Service", "ServiceKey", "foo", "value3");

		// Test lastModified value changes
		long lastModified = s_datastore.getValue("Service", "ServiceKey", "key2").getLastModified().getTime();
		s_datastore.setValue("Service", "ServiceKey", "key2", "changed");
		assertServiceKeyValue("Service", "ServiceKey", "key2", "changed");
		assertThat(s_datastore.getValue("Service", "ServiceKey", "key2").getLastModified().getTime(), greaterThan(lastModified));

		// Test listKeys
		assertThat(s_datastore.listKeys("Service", "ServiceKey"), hasItems("foo", "key1", "key2"));
		assertThat(s_datastore.listKeys("Service", "ServiceKey", "key"), hasItems("key1", "key2"));

		// Test delete
		lastModified = s_datastore.getServiceKeyLastModifiedTime("Service", "ServiceKey").getTime();
		s_datastore.deleteKey("Service", "ServiceKey", "key2");
		assertThat(s_datastore.listKeys("Service", "ServiceKey"), hasItems("key1", "foo"));
		assertThat(s_datastore.getValue("Service", "ServiceKey", "key2"), is(nullValue()));
		assertThat(s_datastore.getServiceKeyLastModifiedTime("Service", "ServiceKey").getTime(), greaterThan(lastModified));

		lastModified = s_datastore.getServiceKeyLastModifiedTime("Service", "ServiceKey").getTime();
		s_datastore.deleteKey("Service", "ServiceKey", "foo");
		assertThat(s_datastore.listKeys("Service", "ServiceKey"), hasItems("key1"));
		assertThat(s_datastore.getValue("Service", "ServiceKey", "foo"), is(nullValue()));
		assertThat(s_datastore.getServiceKeyLastModifiedTime("Service", "ServiceKey").getTime(), greaterThan(lastModified));
	}

	@Test
	public void test_serviceKeyStore_multipleServices()
			throws DatastoreException
	{
		s_datastore.setValue("Service1", "ServiceKey1", "key1", "value1");
		s_datastore.setValue("Service1", "ServiceKey2", "key1", "value2");
		s_datastore.setValue("Service1", "ServiceKey3", "key1", "value3");

		s_datastore.setValue("Service2", "ServiceKey1", "key1", "value4");
		s_datastore.setValue("Service2", "ServiceKey1", "key2", "value5");
		s_datastore.setValue("Service2", "ServiceKey1", "key3", "value6");
		s_datastore.setValue("Service2", "ServiceKey1", "key4", "value7");

		s_datastore.setValue("Service3", "ServiceKey1", "foo", "value8");
		s_datastore.setValue("Service3", "ServiceKey1", "bar", "value9");

		// Test listKeys
		assertThat(s_datastore.listKeys("Service1", "ServiceKey1"), hasItems("key1"));
		assertThat(s_datastore.listKeys("Service1", "ServiceKey2"), hasItems("key1"));
		assertThat(s_datastore.listKeys("Service1", "ServiceKey3"), hasItems("key1"));
		assertThat(s_datastore.listKeys("Service2", "ServiceKey1"), hasItems("key1", "key2", "key3", "key4"));
		assertThat(s_datastore.listKeys("Service3", "ServiceKey1"), hasItems("foo", "bar"));

		// Test listServiceKeys
		assertThat(s_datastore.listServiceKeys("Service1"), hasItems("ServiceKey1", "ServiceKey2", "ServiceKey3"));

		// Test get
		assertServiceKeyValue("Service1", "ServiceKey1", "key1", "value1");
		assertServiceKeyValue("Service1", "ServiceKey2", "key1", "value2");
		assertServiceKeyValue("Service1", "ServiceKey3", "key1", "value3");
		assertServiceKeyValue("Service2", "ServiceKey1", "key1", "value4");
		assertServiceKeyValue("Service2", "ServiceKey1", "key2", "value5");
		assertServiceKeyValue("Service2", "ServiceKey1", "key3", "value6");
		assertServiceKeyValue("Service2", "ServiceKey1", "key4", "value7");
		assertServiceKeyValue("Service3", "ServiceKey1", "foo", "value8");
		assertServiceKeyValue("Service3", "ServiceKey1", "bar", "value9");
	}

	/**
	 * Delete on the last row of a primary key leaves a row with a null (empty) column for the key.
	 * Verify that we ignore this row.
	 */
	@Test
	public void test_serviceKeyStore_nullKey()
			throws DatastoreException
	{
		s_datastore.setValue("Service1", "ServiceKey1", "key1", "value1");
		s_datastore.deleteKey("Service1", "ServiceKey1", "key1");

		assertThat(s_datastore.listKeys("Service1", "ServiceKey1").iterator().hasNext(), equalTo(false));
	}

	private void assertServiceKeyValue(String service, String serviceKey, String key, String expected)
			throws DatastoreException
	{
		ServiceKeyValue value = s_datastore.getValue(service, serviceKey, key);
		assertThat(value.getValue(), equalTo(expected));
		assertThat(value.getLastModified(), is(notNullValue()));
	}
}
