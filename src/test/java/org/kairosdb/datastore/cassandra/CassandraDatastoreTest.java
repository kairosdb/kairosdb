/*
 * Copyright 2013 Proofpoint Inc.
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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.SetMultimap;
import org.hamcrest.CoreMatchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointListener;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datastore.*;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.datastore.DatastoreMetricQueryImpl;
import org.kairosdb.datastore.DatastoreTestHelper;

import java.io.IOException;
import java.util.*;

import static junit.framework.Assert.assertFalse;
import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;


public class CassandraDatastoreTest extends DatastoreTestHelper
{
	public static final String ROW_KEY_TEST_METRIC = "row_key_test_metric";
	public static final String ROW_KEY_BIG_METRIC = "row_key_big_metric";

	private static final int MAX_ROW_READ_SIZE = 1024;
	private static final int OVERFLOW_SIZE = MAX_ROW_READ_SIZE * 2 + 10;

	private static Random random = new Random();
	private static CassandraDatastore s_datastore;
	private static long s_dataPointTime;
	public static final HashMultimap<String,String> EMPTY_MAP = HashMultimap.create();

	private static void loadCassandraData() throws DatastoreException
	{
		s_dataPointTime = System.currentTimeMillis();

		metricNames.add(ROW_KEY_TEST_METRIC);

		DataPointSet dpSet = new DataPointSet(ROW_KEY_TEST_METRIC);
		dpSet.addTag("host", "A");
		dpSet.addTag("client", "foo");

		dpSet.addDataPoint(new DataPoint(s_dataPointTime, 42));

		s_datastore.putDataPoints(dpSet);


		dpSet = new DataPointSet(ROW_KEY_TEST_METRIC);
		dpSet.addTag("host", "B");
		dpSet.addTag("client", "foo");

		dpSet.addDataPoint(new DataPoint(s_dataPointTime, 42));

		s_datastore.putDataPoints(dpSet);


		dpSet = new DataPointSet(ROW_KEY_TEST_METRIC);
		dpSet.addTag("host", "C");
		dpSet.addTag("client", "bar");

		dpSet.addDataPoint(new DataPoint(s_dataPointTime, 42));

		s_datastore.putDataPoints(dpSet);


		dpSet = new DataPointSet(ROW_KEY_TEST_METRIC);
		dpSet.addTag("host", "D");
		dpSet.addTag("client", "bar");

		dpSet.addDataPoint(new DataPoint(s_dataPointTime, 42));

		s_datastore.putDataPoints(dpSet);


		// Add a row of data that is larger than MAX_ROW_READ_SIZE
		dpSet = new DataPointSet(ROW_KEY_BIG_METRIC);
		dpSet.addTag("host", "E");

		for (int i = OVERFLOW_SIZE; i > 0; i--)
		{
			dpSet.addDataPoint(new DataPoint(s_dataPointTime - (long) i, 42));
		}

		s_datastore.putDataPoints(dpSet);


		// NOTE: This data will be deleted by delete tests. Do not expect it to be there.
		metricNames.add("MetricToDelete");
		dpSet = new DataPointSet("MetricToDelete");
		dpSet.addTag("host", "A");
		dpSet.addTag("client", "bar");

		long rowKeyTime = CassandraDatastore.calculateRowTime(s_dataPointTime);
		dpSet.addDataPoint(new DataPoint(rowKeyTime, 13));
		dpSet.addDataPoint(new DataPoint(rowKeyTime + 1000, 14));
		dpSet.addDataPoint(new DataPoint(rowKeyTime + 2000, 15));
		dpSet.addDataPoint(new DataPoint(rowKeyTime + 3000, 16));

		s_datastore.putDataPoints(dpSet);

		// NOTE: This data will be deleted by delete tests. Do not expect it to be there.
		metricNames.add("OtherMetricToDelete");
		dpSet = new DataPointSet("OtherMetricToDelete");
		dpSet.addTag("host", "B");
		dpSet.addTag("client", "bar");

		dpSet.addDataPoint(new DataPoint(rowKeyTime, 13));
		dpSet.addDataPoint(new DataPoint(rowKeyTime + CassandraDatastore.ROW_WIDTH, 14));
		dpSet.addDataPoint(new DataPoint(rowKeyTime + (2 * CassandraDatastore.ROW_WIDTH), 15));
		dpSet.addDataPoint(new DataPoint(rowKeyTime + (3 * CassandraDatastore.ROW_WIDTH), 16));

		s_datastore.putDataPoints(dpSet);

		// NOTE: This data will be deleted by delete tests. Do not expect it to be there.
		metricNames.add("MetricToPartiallyDelete");
		dpSet = new DataPointSet("MetricToPartiallyDelete");
		dpSet.addTag("host", "B");
		dpSet.addTag("client", "bar");

		dpSet.addDataPoint(new DataPoint(rowKeyTime, 13));
		dpSet.addDataPoint(new DataPoint(rowKeyTime + CassandraDatastore.ROW_WIDTH, 14));
		dpSet.addDataPoint(new DataPoint(rowKeyTime + (2 * CassandraDatastore.ROW_WIDTH), 15));
		dpSet.addDataPoint(new DataPoint(rowKeyTime + (3 * CassandraDatastore.ROW_WIDTH), 16));

		s_datastore.putDataPoints(dpSet);

		// NOTE: This data will be deleted by delete tests. Do not expect it to be there.
		metricNames.add("YetAnotherMetricToDelete");
		dpSet = new DataPointSet("YetAnotherMetricToDelete");
		dpSet.addTag("host", "A");
		dpSet.addTag("client", "bar");

		dpSet.addDataPoint(new DataPoint(rowKeyTime, 13));
		dpSet.addDataPoint(new DataPoint(rowKeyTime + 1000, 14));
		dpSet.addDataPoint(new DataPoint(rowKeyTime + 2000, 15));
		dpSet.addDataPoint(new DataPoint(rowKeyTime + 3000, 16));

		s_datastore.putDataPoints(dpSet);
	}

	@BeforeClass
	public static void setupDatastore() throws InterruptedException, DatastoreException
	{
		s_datastore = new CassandraDatastore("localhost:9160",
				null, 1, MAX_ROW_READ_SIZE, MAX_ROW_READ_SIZE, MAX_ROW_READ_SIZE, 1000, 50000, "hostname");

		DatastoreTestHelper.s_datastore = new KairosDatastore(s_datastore,
				new QueryQueuingManager(1, "hostname"),
				Collections.<DataPointListener>emptyList(), "hostname");

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

	private static void deleteMetric(String metricName) throws IOException, DatastoreException
	{
		DatastoreMetricQueryImpl query = new DatastoreMetricQueryImpl(metricName, EMPTY_MAP, 0L, Long.MAX_VALUE);
		s_datastore.deleteDataPoints(query, createCache("YetAnotherMetricToDelete"));
	}

	@Test
	public void test_getKeysForQuery()
	{
		DatastoreMetricQuery query = new DatastoreMetricQueryImpl(ROW_KEY_TEST_METRIC,
				HashMultimap.<String, String>create(), s_dataPointTime, s_dataPointTime);

		ListMultimap<Long, DataPointsRowKey> keys = s_datastore.getKeysForQuery(query);

		assertEquals(4, keys.size());
	}

	@Test
	public void test_getKeysForQuery_withFilter()
	{
		SetMultimap<String, String> tagFilter = HashMultimap.create();
		tagFilter.put("client", "bar");

		DatastoreMetricQuery query = new DatastoreMetricQueryImpl(ROW_KEY_TEST_METRIC,
				tagFilter, s_dataPointTime, s_dataPointTime);

		ListMultimap<Long, DataPointsRowKey> keys = s_datastore.getKeysForQuery(query);

		assertEquals(2, keys.size());
	}

	@Test
	public void test_rowLargerThanMaxReadSize() throws DatastoreException
	{
		Map<String, String> tagFilter = new HashMap<String, String>();
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
		s_datastore.deleteDataPoints(null, createCache("foo"));
	}

	@Test (expected = NullPointerException.class)
	public void test_deleteDataPoints_nullCache_Invalid() throws IOException, DatastoreException
	{
		DatastoreMetricQuery query = new DatastoreMetricQueryImpl("MetricToDelete", EMPTY_MAP, 0L, Long.MAX_VALUE);
		s_datastore.deleteDataPoints(query, null);
	}

	@Test
	public void test_deleteDataPoints_DeleteEntireRow() throws IOException, DatastoreException, InterruptedException
	{
		String metricToDelete = "MetricToDelete";
		DatastoreMetricQuery query = new DatastoreMetricQueryImpl(metricToDelete, EMPTY_MAP, 0L, Long.MAX_VALUE);

		List<DataPointRow> rows = s_datastore.queryDatabase(query, createCache(metricToDelete));
		assertThat(rows.size(), equalTo(1));

		s_datastore.deleteDataPoints(query, createCache(metricToDelete));
		Thread.sleep(2000);

		// Verify that all data points are gone
		rows = s_datastore.queryDatabase(query, createCache(metricToDelete));
		assertThat(rows.size(), equalTo(0));

		// Verify that the index key is gone
		ListMultimap<Long, DataPointsRowKey> indexRowKeys = s_datastore.getKeysForQuery(query);
		assertThat(indexRowKeys.size(), equalTo(0));

		// Verify that the metric name is gone from the Strings column family
		assertThat(s_datastore.getMetricNames(), not(hasItem(metricToDelete)));
	}

	@Test
	public void test_deleteDataPoints_DeleteColumnsSpanningRows() throws IOException, DatastoreException, InterruptedException
	{
		String metricToDelete = "OtherMetricToDelete";
		DatastoreMetricQuery query = new DatastoreMetricQueryImpl(metricToDelete, EMPTY_MAP, 0L, Long.MAX_VALUE);

		List<DataPointRow> rows = s_datastore.queryDatabase(query, createCache(metricToDelete));
		assertThat(rows.size(), equalTo(4));

		s_datastore.deleteDataPoints(query, createCache(metricToDelete));
		Thread.sleep(2000);

		rows = s_datastore.queryDatabase(query, createCache(metricToDelete));
		assertThat(rows.size(), equalTo(0));

		// Verify that the index key is gone
		DatastoreMetricQueryImpl queryEverything = new DatastoreMetricQueryImpl(metricToDelete, EMPTY_MAP, 0L, Long.MAX_VALUE);
		ListMultimap<Long, DataPointsRowKey> indexRowKeys = s_datastore.getKeysForQuery(queryEverything);
		assertThat(indexRowKeys.size(), equalTo(0));

		// Verify that the metric name is gone from the Strings column family
		assertThat(s_datastore.getMetricNames(), not(hasItem(metricToDelete)));
	}

	@Test
	public void test_deleteDataPoints_DeleteColumnsSpanningRows_rowsLeft() throws IOException, DatastoreException, InterruptedException
	{
		long rowKeyTime = CassandraDatastore.calculateRowTime(s_dataPointTime);
		String metricToDelete = "MetricToPartiallyDelete";
		DatastoreMetricQuery query = new DatastoreMetricQueryImpl(metricToDelete, EMPTY_MAP, 0L, Long.MAX_VALUE);

		List<DataPointRow> rows = s_datastore.queryDatabase(query, createCache(metricToDelete));
		assertThat(rows.size(), equalTo(4));

		DatastoreMetricQuery deleteQuery = new DatastoreMetricQueryImpl(metricToDelete, EMPTY_MAP, 0L,
				rowKeyTime + (3 * CassandraDatastore.ROW_WIDTH - 1));
		s_datastore.deleteDataPoints(deleteQuery, createCache(metricToDelete));
		Thread.sleep(2000);

		rows = s_datastore.queryDatabase(query, createCache(metricToDelete));
		assertThat(rows.size(), equalTo(1));

		// Verify that the index key is gone
		DatastoreMetricQueryImpl queryEverything = new DatastoreMetricQueryImpl(metricToDelete, EMPTY_MAP, 0L, Long.MAX_VALUE);
		ListMultimap<Long, DataPointsRowKey> indexRowKeys = s_datastore.getKeysForQuery(queryEverything);
		assertThat(indexRowKeys.size(), equalTo(1));

		// Verify that the metric name still exists in the Strings column family
		assertThat(s_datastore.getMetricNames(), hasItem(metricToDelete));
	}

	@Test
	public void test_deleteDataPoints_DeleteColumnWithinRow() throws IOException, DatastoreException, InterruptedException
	{
		long rowKeyTime = CassandraDatastore.calculateRowTime(s_dataPointTime);
		String metricToDelete = "YetAnotherMetricToDelete";
		DatastoreMetricQuery query = new DatastoreMetricQueryImpl(metricToDelete, EMPTY_MAP, rowKeyTime, rowKeyTime + 2000);

		List<DataPointRow> rows = s_datastore.queryDatabase(query, createCache(metricToDelete));
		assertThat(rows.size(), equalTo(1));

		s_datastore.deleteDataPoints(query, createCache(metricToDelete));
		Thread.sleep(2000);

		rows = s_datastore.queryDatabase(query, createCache(metricToDelete));
		assertThat(rows.size(), equalTo(1));
		DataPointRow row = rows.get(0);
		int count = 0;
		while(row.hasNext())
		{
			DataPoint next = row.next();
			assertThat(next.getLongValue(), equalTo(16L));
			count++;
		}

		assertThat(count, equalTo(0));

		// Verify that the index key is gone
		DatastoreMetricQueryImpl queryEverything = new DatastoreMetricQueryImpl(metricToDelete, EMPTY_MAP, 0L, Long.MAX_VALUE);
		ListMultimap<Long, DataPointsRowKey> indexRowKeys = s_datastore.getKeysForQuery(queryEverything);
		assertThat(indexRowKeys.size(), equalTo(1));

		// Verify that the metric name still exists in the Strings column family
		assertThat(s_datastore.getMetricNames(), hasItem(metricToDelete));
	}

	/**
	 This is here because hbase throws an exception in this case
	 @throws DatastoreException
	 */
	@Test
	public void test_queryDatabase_noMetric() throws DatastoreException
	{

		Map<String, String> tags = new TreeMap<String, String>();
		QueryMetric query = new QueryMetric(500, 0, "metric_not_there");
		query.setEndTime(3000);

		query.setTags(tags);

		DatastoreQuery dq = super.s_datastore.createQuery(query);

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
		set.addDataPoint(new DataPoint(1, 1L));
		set.addDataPoint(new DataPoint(2, 2L));
		set.addDataPoint(new DataPoint(0, 3L));
		set.addDataPoint(new DataPoint(3, 4L));
		set.addDataPoint(new DataPoint(4, 5L));
		set.addDataPoint(new DataPoint(5, 6L));
		s_datastore.putDataPoints(set);
	}

	@Test(expected = DatastoreException.class)
	public void test_TimestampsNegative() throws DatastoreException
	{
		DataPointSet set = new DataPointSet("testMetric");
		set.addDataPoint(new DataPoint(-1, 1L));
		s_datastore.putDataPoints(set);
	}

	private static CachedSearchResult createCache(String metricName) throws IOException
	{
		String tempFile = System.getProperty("java.io.tmpdir");
		return CachedSearchResult.createCachedSearchResult(metricName, tempFile + "/" + random.nextLong());
	}
}
