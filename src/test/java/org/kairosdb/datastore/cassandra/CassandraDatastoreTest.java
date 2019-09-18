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
import com.google.common.collect.SetMultimap;
import io.opentracing.mock.MockTracer;
import org.hamcrest.CoreMatchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.kairosdb.core.*;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;
import org.kairosdb.core.datastore.*;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.datastore.DatastoreMetricQueryImpl;
import org.kairosdb.datastore.DatastoreTestHelper;
import org.kairosdb.datastore.cassandra.cache.RowKeyCache;
import org.kairosdb.datastore.cassandra.cache.StringKeyCache;

import java.io.IOException;
import java.util.*;

import static junit.framework.Assert.assertFalse;
import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;


@Ignore
public class CassandraDatastoreTest extends DatastoreTestHelper {
    public static final String ROW_KEY_TEST_METRIC = "row_key_test_metric";
    public static final String ROW_KEY_BIG_METRIC = "row_key_big_metric";

    private static final int MAX_ROW_READ_SIZE = 1024;
    private static final int OVERFLOW_SIZE = MAX_ROW_READ_SIZE * 2 + 10;

    private static KairosDataPointFactory dataPointFactory = new TestDataPointFactory();
    private static LongDataPointFactory longDataPointFactory = new LongDataPointFactoryImpl();
    private static Random random = new Random();
    private static CassandraDatastore s_datastore;
    private static long s_dataPointTime;
    public static final HashMultimap<String, String> EMPTY_MAP = HashMultimap.create();
    private static MockTracer tracer = new MockTracer();

    private static void putDataPoints(DataPointSet dps) throws DatastoreException {
        for (DataPoint dataPoint : dps.getDataPoints()) {
            s_datastore.putDataPoint(dps.getName(), dps.getTags(), dataPoint, 0);
        }
    }

    private static void loadCassandraData() throws DatastoreException {
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

        for (int i = OVERFLOW_SIZE; i > 0; i--) {
            dpSet.addDataPoint(new LongDataPoint(s_dataPointTime - (long) i, 42));
        }

        putDataPoints(dpSet);


        // NOTE: This data will be deleted by delete tests. Do not expect it to be there.
        metricNames.add("MetricToDelete");
        dpSet = new DataPointSet("MetricToDelete");
        dpSet.addTag("host", "A");
        dpSet.addTag("client", "bar");

        long rowKeyTime = s_datastore.calculateRowTimeWrite(s_dataPointTime);
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
        dpSet.addDataPoint(new LongDataPoint(rowKeyTime + s_datastore.getRowWidthWrite(), 14));
        dpSet.addDataPoint(new LongDataPoint(rowKeyTime + (2 * s_datastore.getRowWidthWrite()), 15));
        dpSet.addDataPoint(new LongDataPoint(rowKeyTime + (3 * s_datastore.getRowWidthWrite()), 16));

        putDataPoints(dpSet);

        // NOTE: This data will be deleted by delete tests. Do not expect it to be there.
        metricNames.add("MetricToPartiallyDelete");
        dpSet = new DataPointSet("MetricToPartiallyDelete");
        dpSet.addTag("host", "B");
        dpSet.addTag("client", "bar");

        dpSet.addDataPoint(new LongDataPoint(rowKeyTime, 13));
        dpSet.addDataPoint(new LongDataPoint(rowKeyTime + s_datastore.getRowWidthWrite(), 14));
        dpSet.addDataPoint(new LongDataPoint(rowKeyTime + (2 * s_datastore.getRowWidthWrite()), 15));
        dpSet.addDataPoint(new LongDataPoint(rowKeyTime + (3 * s_datastore.getRowWidthWrite()), 16));

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
    }

    @BeforeClass
    public static void setupDatastore() throws InterruptedException, DatastoreException {
        String cassandraHost = "localhost";
        if (System.getenv("CASSANDRA_HOST") != null) {
            cassandraHost = System.getenv("CASSANDRA_HOST");
        }

        System.out.println("Starting Cassandra Connection: " + cassandraHost);

        CassandraConfiguration cassandraConfig = new CassandraConfiguration(1, MAX_ROW_READ_SIZE, MAX_ROW_READ_SIZE, MAX_ROW_READ_SIZE,
                1000, 50000, cassandraHost, "kairosdb_test");

        // TODO: test the caches being hit
        final StringKeyCache stringCache = mock(StringKeyCache.class);
        s_datastore = new CassandraDatastore(new CassandraClientImpl(cassandraConfig), cassandraConfig,
                dataPointFactory, longDataPointFactory, mock(RowKeyCache.class), stringCache, stringCache, tracer);

        System.out.println("Creating KairosDataStore");
        DatastoreTestHelper.s_datastore = new KairosDatastore(s_datastore,
                new QueryQueuingManager(1, "hostname"),
                Collections.<DataPointListener>emptyList(), dataPointFactory, tracer);

        System.out.println("Loading Cassandra data");
        loadCassandraData();
        System.out.println("Loading data");
        loadData();
        Thread.sleep(2000);
    }

    @AfterClass
    public static void closeDatastore() throws InterruptedException, IOException, DatastoreException {
        for (String metricName : metricNames) {
            deleteMetric(metricName);
        }

        s_datastore.close();
    }

    private static List<DataPointsRowKey> readIterator(Iterator<DataPointsRowKey> it) {
        List<DataPointsRowKey> ret = new ArrayList<>();
        while (it.hasNext())
            ret.add(it.next());

        return (ret);
    }

    private static void deleteMetric(String metricName) throws IOException, DatastoreException {
        DatastoreMetricQueryImpl query = new DatastoreMetricQueryImpl(metricName, EMPTY_MAP, 0L, Long.MAX_VALUE);
        s_datastore.deleteDataPoints(query);
    }

    @Test
    public void test_getKeysForQuery() {
        DatastoreMetricQuery query = new DatastoreMetricQueryImpl(ROW_KEY_TEST_METRIC,
                HashMultimap.<String, String>create(), s_dataPointTime, s_dataPointTime);

        Collection<DataPointsRowKey> keys = s_datastore.getKeysForQueryIterator(query);

        assertEquals(4, keys.size());
    }

    @Test
    public void test_getKeysForQuery_withFilter() {
        SetMultimap<String, String> tagFilter = HashMultimap.create();
        tagFilter.put("client", "bar");

        DatastoreMetricQuery query = new DatastoreMetricQueryImpl(ROW_KEY_TEST_METRIC,
                tagFilter, s_dataPointTime, s_dataPointTime);

        Collection<DataPointsRowKey> keys = s_datastore.getKeysForQueryIterator(query);

        assertEquals(2, keys.size());
    }

    @Test
    public void test_rowLargerThanMaxReadSize() throws DatastoreException {
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
        while (dataPointGroup.hasNext()) {
            DataPoint dp = dataPointGroup.next();
            total += dp.getLongValue();
            counter++;
        }

        dataPointGroup.close();
        assertThat(total, equalTo(counter * 42));
        assertEquals(OVERFLOW_SIZE, counter);
        dq.close();
    }

    @Test(expected = NullPointerException.class)
    public void test_deleteDataPoints_nullQuery_Invalid() throws IOException, DatastoreException {
        s_datastore.deleteDataPoints(null);
    }

    @Ignore
    @Test
    public void test_deleteDataPoints_DeleteEntireRow() throws IOException, DatastoreException, InterruptedException {
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
        Collection<DataPointsRowKey> indexRowKeys = s_datastore.getKeysForQueryIterator(query);
        assertThat(indexRowKeys.size(), equalTo(0));

        // Verify that the metric name is gone from the Strings column family
        assertThat(s_datastore.getMetricNames(), not(hasItem(metricToDelete)));
    }

    @Ignore
    @Test
    public void test_deleteDataPoints_DeleteColumnsSpanningRows() throws IOException, DatastoreException, InterruptedException {
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
        Collection<DataPointsRowKey> indexRowKeys = s_datastore.getKeysForQueryIterator(queryEverything);
        assertThat(indexRowKeys.size(), equalTo(0));

        // Verify that the metric name is gone from the Strings column family
        assertThat(s_datastore.getMetricNames(), not(hasItem(metricToDelete)));
    }

    @Ignore
    @Test
    public void test_deleteDataPoints_DeleteColumnsSpanningRows_rowsLeft() throws IOException, DatastoreException, InterruptedException {
        long rowKeyTime = s_datastore.calculateRowTimeRead(s_dataPointTime);
        String metricToDelete = "MetricToPartiallyDelete";
        DatastoreMetricQuery query = new DatastoreMetricQueryImpl(metricToDelete, EMPTY_MAP, 0L, Long.MAX_VALUE);

        CachedSearchResult res = createCache(metricToDelete);
        s_datastore.queryDatabase(query, res);
        List<DataPointRow> rows = res.getRows();
        assertThat(rows.size(), equalTo(4));

        DatastoreMetricQuery deleteQuery = new DatastoreMetricQueryImpl(metricToDelete, EMPTY_MAP, 0L,
                rowKeyTime + (3 * s_datastore.getRowWidthRead() - 1));
        s_datastore.deleteDataPoints(deleteQuery);
        Thread.sleep(2000);

        res = createCache(metricToDelete);
        s_datastore.queryDatabase(query, res);
        rows = res.getRows();
        assertThat(rows.size(), equalTo(1));

        // Verify that the index key is gone
        DatastoreMetricQueryImpl queryEverything = new DatastoreMetricQueryImpl(metricToDelete, EMPTY_MAP, 0L, Long.MAX_VALUE);
        Collection<DataPointsRowKey> indexRowKeys = s_datastore.getKeysForQueryIterator(queryEverything);
        assertThat(indexRowKeys.size(), equalTo(1));

        // Verify that the metric name still exists in the Strings column family
        assertThat(s_datastore.getMetricNames(), hasItem(metricToDelete));
    }

    @Ignore
    @Test
    public void test_deleteDataPoints_DeleteColumnWithinRow() throws IOException, DatastoreException, InterruptedException {
        long rowKeyTime = s_datastore.calculateRowTimeRead(s_dataPointTime);
        String metricToDelete = "YetAnotherMetricToDelete";
        DatastoreMetricQuery query = new DatastoreMetricQueryImpl(metricToDelete, EMPTY_MAP, rowKeyTime, rowKeyTime + 2000);

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
        Collection<DataPointsRowKey> indexRowKeys = s_datastore.getKeysForQueryIterator(queryEverything);
        assertThat(indexRowKeys.size(), equalTo(1));

        // Verify that the metric name still exists in the Strings column family
        assertThat(s_datastore.getMetricNames(), hasItem(metricToDelete));
    }

    /**
     * This is here because hbase throws an exception in this case
     *
     * @throws DatastoreException
     */
    @Test
    public void test_queryDatabase_noMetric() throws DatastoreException {

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
    public void test_TimestampsCloseToZero() throws DatastoreException {
        DataPointSet set = new DataPointSet("testMetric");
        set.addDataPoint(new LongDataPoint(1, 1L));
        set.addDataPoint(new LongDataPoint(2, 2L));
        set.addDataPoint(new LongDataPoint(0, 3L));
        set.addDataPoint(new LongDataPoint(3, 4L));
        set.addDataPoint(new LongDataPoint(4, 5L));
        set.addDataPoint(new LongDataPoint(5, 6L));
        putDataPoints(set);
    }

    private static CachedSearchResult createCache(String metricName) throws IOException {
        String tempFile = System.getProperty("java.io.tmpdir");
        return CachedSearchResult.createCachedSearchResult(metricName,
                tempFile + "/" + random.nextLong(), dataPointFactory);
    }

    @Test
    public void test_setTTL() throws DatastoreException, InterruptedException {
        DataPointSet set = new DataPointSet("ttlMetric");
        set.addTag("tag", "value");
        set.addDataPoint(new LongDataPoint(1, 1L));
        set.addDataPoint(new LongDataPoint(2, 2L));
        set.addDataPoint(new LongDataPoint(0, 3L));
        set.addDataPoint(new LongDataPoint(3, 4L));
        set.addDataPoint(new LongDataPoint(4, 5L));
        set.addDataPoint(new LongDataPoint(5, 6L));
        putDataPoints(set);

        s_datastore.putDataPoint("ttlMetric", set.getTags(),
                new LongDataPoint(50, 7L), 1);

        Thread.sleep(2000);
        Map<String, String> tags = new TreeMap<>();
        QueryMetric query = new QueryMetric(0, 500, 0, "ttlMetric");

        query.setTags(tags);

        DatastoreQuery dq = super.s_datastore.createQuery(query);

        List<DataPointGroup> results = dq.execute();
        try {
            assertThat(results.size(), CoreMatchers.equalTo(1));
            DataPointGroup dpg = results.get(0);
            assertThat(dpg.getName(), is("ttlMetric"));
            assertThat(dq.getSampleSize(), CoreMatchers.equalTo(6));
        } finally {
            dq.close();
        }
    }
}
