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
package org.kairosdb.core.datastore;

import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.TestDataPointFactory;
import org.kairosdb.core.aggregator.AggregatorFactory;
import org.kairosdb.core.aggregator.TestAggregatorFactory;
import org.kairosdb.core.datapoints.LegacyDataPointFactory;
import org.kairosdb.core.datapoints.LegacyLongDataPoint;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.core.formatter.FormatterException;
import org.kairosdb.core.groupby.GroupByResult;
import org.kairosdb.core.groupby.TagGroupBy;
import org.kairosdb.core.groupby.TagGroupByResult;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.junit.Assert.assertThat;

public class KairosDatastoreTest
{
	private AggregatorFactory aggFactory;

	public KairosDatastoreTest() throws KairosDBException
	{
		aggFactory = new TestAggregatorFactory();
	}

	@Test(expected = NullPointerException.class)
	public void test_query_nullMetricInvalid() throws KairosDBException
	{
		TestDatastore testds = new TestDatastore();
		KairosDatastore datastore = new KairosDatastore(testds, new QueryQueuingManager(1, "hostname"),
				new TestDataPointFactory(), false);

		datastore.createQuery(null);
	}

	@Test
	public void test_query_sumAggregator() throws KairosDBException
	{
		TestDatastore testds = new TestDatastore();
		KairosDatastore datastore = new KairosDatastore(testds, new QueryQueuingManager(1, "hostname"),
				new TestDataPointFactory(), false);
		QueryMetric metric = new QueryMetric(1L, 1, "metric1");
		metric.addAggregator(aggFactory.createAggregator("sum"));

		DatastoreQuery dq = datastore.createQuery(metric);
		List<DataPointGroup> results = dq.execute();

		DataPointGroup group = results.get(0);

		DataPoint dataPoint = group.next();
		assertThat(dataPoint.getTimestamp(), equalTo(1L));
		assertThat(dataPoint.getLongValue(), equalTo(72L));

		dataPoint = group.next();
		assertThat(dataPoint.getTimestamp(), equalTo(2L));
		assertThat(dataPoint.getLongValue(), equalTo(32L));

		dataPoint = group.next();
		assertThat(dataPoint.getTimestamp(), equalTo(3L));
		assertThat(dataPoint.getLongValue(), equalTo(32L));

		dq.close();
	}

	@Test
	public void test_query_noAggregator() throws KairosDBException
	{
		TestDatastore testds = new TestDatastore();
		KairosDatastore datastore = new KairosDatastore(testds, new QueryQueuingManager(1, "hostname"),
				new TestDataPointFactory(), false);
		QueryMetric metric = new QueryMetric(1L, 1, "metric1");

		DatastoreQuery dq = datastore.createQuery(metric);
		List<DataPointGroup> results = dq.execute();

		assertThat(results.size(), is(1));
		DataPointGroup group = results.get(0);

		DataPoint dataPoint = group.next();
		assertThat(dataPoint.getTimestamp(), equalTo(1L));
		assertThat(dataPoint.getLongValue(), equalTo(3L));

		dataPoint = group.next();
		assertThat(dataPoint.getTimestamp(), equalTo(1L));
		assertThat(dataPoint.getLongValue(), equalTo(5L));

		dataPoint = group.next();
		assertThat(dataPoint.getTimestamp(), equalTo(1L));
		assertThat(dataPoint.getLongValue(), equalTo(10L));

		dataPoint = group.next();
		assertThat(dataPoint.getTimestamp(), equalTo(1L));
		assertThat(dataPoint.getLongValue(), equalTo(14L));

		dataPoint = group.next();
		assertThat(dataPoint.getTimestamp(), equalTo(1L));
		assertThat(dataPoint.getLongValue(), equalTo(20L));

		dataPoint = group.next();
		assertThat(dataPoint.getTimestamp(), equalTo(1L));
		assertThat(dataPoint.getLongValue(), equalTo(20L));

		dataPoint = group.next();
		assertThat(dataPoint.getTimestamp(), equalTo(2L));
		assertThat(dataPoint.getLongValue(), equalTo(1L));

		dataPoint = group.next();
		assertThat(dataPoint.getTimestamp(), equalTo(2L));
		assertThat(dataPoint.getLongValue(), equalTo(3L));

		dataPoint = group.next();
		assertThat(dataPoint.getTimestamp(), equalTo(2L));
		assertThat(dataPoint.getLongValue(), equalTo(5L));

		dataPoint = group.next();
		assertThat(dataPoint.getTimestamp(), equalTo(2L));
		assertThat(dataPoint.getLongValue(), equalTo(6L));

		dataPoint = group.next();
		assertThat(dataPoint.getTimestamp(), equalTo(2L));
		assertThat(dataPoint.getLongValue(), equalTo(8L));

		dataPoint = group.next();
		assertThat(dataPoint.getTimestamp(), equalTo(2L));
		assertThat(dataPoint.getLongValue(), equalTo(9L));

		dataPoint = group.next();
		assertThat(dataPoint.getTimestamp(), equalTo(3L));
		assertThat(dataPoint.getLongValue(), equalTo(7L));

		dataPoint = group.next();
		assertThat(dataPoint.getTimestamp(), equalTo(3L));
		assertThat(dataPoint.getLongValue(), equalTo(25L));

		dq.close();
	}

	@SuppressWarnings({"ResultOfMethodCallIgnored", "ConstantConditions"})
	@Test
	public void test_cleanCacheDir() throws IOException, DatastoreException
	{
		TestDatastore testds = new TestDatastore();
		KairosDatastore datastore = new KairosDatastore(testds, new QueryQueuingManager(1, "hostname"),
				new TestDataPointFactory(), false);

		// Create files in the cache directory
		File cacheDir = new File(datastore.getCacheDir());
		File file1 = new File(cacheDir, "testFile1");
		file1.createNewFile();
		File file2 = new File(cacheDir, "testFile2");
		file2.createNewFile();

		File[] files = cacheDir.listFiles();
		assertTrue(files.length > 0);

		datastore.cleanCacheDir(false);

		assertFalse(file1.exists());
		assertFalse(file2.exists());
	}

	@Test
	public void test_groupByTypeAndTag_SameTagValue() throws DatastoreException, FormatterException
	{
		TestKairosDatastore datastore = new TestKairosDatastore(new TestDatastore(), new QueryQueuingManager(1, "hostname"),
				new TestDataPointFactory());

		TagGroupBy groupBy = new TagGroupBy("tag1", "tag2");
		List<DataPointRow> rows = new ArrayList<>();

		DataPointRowImpl row1 = new DataPointRowImpl();
		row1.addTag("tag1", "value");
		row1.addDataPoint(new LongDataPoint(1234, 1));

		DataPointRowImpl row2 = new DataPointRowImpl();
		row2.addTag("tag2", "value");
		row2.addDataPoint(new LongDataPoint(1235, 2));

		rows.add(row1);
		rows.add(row2);

		List<DataPointGroup> dataPointsGroup = datastore.groupByTypeAndTag("metricName", rows, groupBy, Order.ASC);

		assertThat(dataPointsGroup.size(), equalTo(2));

		assertThat(getTagGroupMap(dataPointsGroup.get(0)), hasEntry("tag1", ""));
		assertThat(getTagGroupMap(dataPointsGroup.get(0)), hasEntry("tag2", "value"));

		assertThat(getTagGroupMap(dataPointsGroup.get(1)), hasEntry("tag1", "value"));
		assertThat(getTagGroupMap(dataPointsGroup.get(1)), hasEntry("tag2", ""));
	}

	@Test
	public void test_groupByTypeAndTag_DifferentTagValues() throws DatastoreException, FormatterException
	{
		TestKairosDatastore datastore = new TestKairosDatastore(new TestDatastore(), new QueryQueuingManager(1, "hostname"),
				new TestDataPointFactory());

		TagGroupBy groupBy = new TagGroupBy("tag1", "tag2");
		List<DataPointRow> rows = new ArrayList<>();

		DataPointRowImpl row1 = new DataPointRowImpl();
		row1.addTag("tag1", "value1");
		row1.addDataPoint(new LongDataPoint(1234, 1));

		DataPointRowImpl row2 = new DataPointRowImpl();
		row2.addTag("tag2", "value2");
		row2.addDataPoint(new LongDataPoint(1235, 2));

		rows.add(row1);
		rows.add(row2);

		List<DataPointGroup> dataPoints = datastore.groupByTypeAndTag("metricName", rows, groupBy, Order.ASC);

		assertThat(dataPoints.size(), equalTo(2));

		assertThat(getTagGroupMap(dataPoints.get(0)), hasEntry("tag1", ""));
		assertThat(getTagGroupMap(dataPoints.get(0)), hasEntry("tag2", "value2"));

		assertThat(getTagGroupMap(dataPoints.get(1)), hasEntry("tag1", "value1"));
		assertThat(getTagGroupMap(dataPoints.get(1)), hasEntry("tag2", ""));
	}

	@Test
	public void test_groupByTypeAndTag_MultipleTags() throws DatastoreException, FormatterException
	{
		TestKairosDatastore datastore = new TestKairosDatastore(new TestDatastore(), new QueryQueuingManager(1, "hostname"),
				new TestDataPointFactory());

		/*
		The order of the returned data must be stored first by tag1 and
		then by tag 2 as specified in the caller group by.
		 */
		TagGroupBy groupBy = new TagGroupBy("tag1", "tag2");
		List<DataPointRow> rows = new ArrayList<>();

		DataPointRowImpl row1 = new DataPointRowImpl();
		row1.addTag("tag1", "value1");
		row1.addTag("tag2", "value2");
		row1.addDataPoint(new LongDataPoint(1234, 1));

		DataPointRowImpl row2 = new DataPointRowImpl();
		row2.addTag("tag1", "value1");
		row2.addTag("tag2", "value3");
		row2.addDataPoint(new LongDataPoint(1235, 2));

		DataPointRowImpl row3 = new DataPointRowImpl();
		row3.addTag("tag1", "value4");
		row3.addTag("tag2", "value2");
		row3.addDataPoint(new LongDataPoint(1235, 2));

		rows.add(row1);
		rows.add(row2);
		rows.add(row3);

		List<DataPointGroup> dataPoints = datastore.groupByTypeAndTag("metricName", rows, groupBy, Order.ASC);

		assertThat(dataPoints.size(), equalTo(3));

		assertThat(getTagGroupMap(dataPoints.get(0)), hasEntry("tag1", "value1"));
		assertThat(getTagGroupMap(dataPoints.get(0)), hasEntry("tag2", "value2"));

		assertThat(getTagGroupMap(dataPoints.get(1)), hasEntry("tag1", "value1"));
		assertThat(getTagGroupMap(dataPoints.get(1)), hasEntry("tag2", "value3"));

		assertThat(getTagGroupMap(dataPoints.get(2)), hasEntry("tag1", "value4"));
		assertThat(getTagGroupMap(dataPoints.get(2)), hasEntry("tag2", "value2"));

	}

	private Map<String, String> getTagGroupMap(DataPointGroup dataPointGroup)
	{
		for (GroupByResult groupByResult : dataPointGroup.getGroupByResult())
		{
			if (groupByResult instanceof TagGroupByResult)
				return ((TagGroupByResult) groupByResult).getTagResults();
		}

		return null;
	}

	private static class TestKairosDatastore extends KairosDatastore
	{

		public TestKairosDatastore(Datastore datastore, QueryQueuingManager queuingManager,
		                           KairosDataPointFactory dataPointFactory) throws DatastoreException
		{
			super(datastore, queuingManager, dataPointFactory, false);
		}
	}

	private static class TestDatastore implements Datastore
	{
		private DatastoreException m_toThrow = null;

		protected TestDatastore() throws DatastoreException
		{
		}

		@Override
		public void close() throws InterruptedException
		{
		}

		@Override
		public Iterable<String> getMetricNames()
		{
			return null;
		}

		@Override
		public Iterable<String> getTagNames()
		{
			return null;
		}

		@Override
		public Iterable<String> getTagValues()
		{
			return null;
		}

		public void throwQueryException(DatastoreException toThrow)
		{
			m_toThrow = toThrow;
		}

		@Override
		public void queryDatabase(DatastoreMetricQuery query, QueryCallback queryCallback)
				throws DatastoreException
		{
			if (m_toThrow != null)
				throw m_toThrow;

			try
			{
				queryCallback.startDataPointSet(LegacyDataPointFactory.DATASTORE_TYPE, Collections.<String, String>emptyMap());
				queryCallback.addDataPoint(new LegacyLongDataPoint(1, 3));
				queryCallback.addDataPoint(new LegacyLongDataPoint(1, 10));
				queryCallback.addDataPoint(new LegacyLongDataPoint(1, 20));
				queryCallback.addDataPoint(new LegacyLongDataPoint(2, 1));
				queryCallback.addDataPoint(new LegacyLongDataPoint(2, 3));
				queryCallback.addDataPoint(new LegacyLongDataPoint(2, 5));
				queryCallback.addDataPoint(new LegacyLongDataPoint(3, 25));

				queryCallback.startDataPointSet(LegacyDataPointFactory.DATASTORE_TYPE, Collections.<String, String>emptyMap());
				queryCallback.addDataPoint(new LegacyLongDataPoint(1, 5));
				queryCallback.addDataPoint(new LegacyLongDataPoint(1, 14));
				queryCallback.addDataPoint(new LegacyLongDataPoint(1, 20));
				queryCallback.addDataPoint(new LegacyLongDataPoint(2, 6));
				queryCallback.addDataPoint(new LegacyLongDataPoint(2, 8));
				queryCallback.addDataPoint(new LegacyLongDataPoint(2, 9));
				queryCallback.addDataPoint(new LegacyLongDataPoint(3, 7));

				queryCallback.endDataPoints();
			}
			catch (IOException e)
			{
				throw new DatastoreException(e);
			}
		}

		@Override
		public void deleteDataPoints(DatastoreMetricQuery deleteQuery) throws DatastoreException
		{
		}

		@Override
		public TagSet queryMetricTags(DatastoreMetricQuery query) throws DatastoreException
		{
			return null;  //To change body of implemented methods use File | Settings | File Templates.
		}
	}
}