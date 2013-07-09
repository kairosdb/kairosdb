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
package org.kairosdb.core.datastore;

import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointListener;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.aggregator.AggregatorFactory;
import org.kairosdb.core.aggregator.TestAggregatorFactory;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.exception.KariosDBException;
import org.kairosdb.testing.TestingDataPointRowImpl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class KairosDatastoreTest
{
	private AggregatorFactory aggFactory = new TestAggregatorFactory();

	@Test(expected = NullPointerException.class)
	public void test_query_nullMetricInvalid() throws KariosDBException
	{
		TestDatastore testds = new TestDatastore();
		KairosDatastore datastore = new KairosDatastore(testds, new QueryQueuingManager(1, "hostname"), Collections.<DataPointListener>emptyList());

		datastore.query(null);
	}

	@Test
	public void test_query_sumAggregator() throws KariosDBException
	{
		TestDatastore testds = new TestDatastore();
		KairosDatastore datastore = new KairosDatastore(testds, new QueryQueuingManager(1, "hostname"), Collections.<DataPointListener>emptyList());
		QueryMetric metric = new QueryMetric(1L, 1, "metric1");
		metric.addAggregator(aggFactory.createAggregator("sum"));

		QueryResults queryResults = datastore.query(metric);
		List<DataPointGroup> results = queryResults.getDataPoints();

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

		queryResults.close();
	}

	@Test
	public void test_query_noAggregator() throws KariosDBException
	{
		TestDatastore testds = new TestDatastore();
		KairosDatastore datastore = new KairosDatastore(testds, new QueryQueuingManager(1, "hostname"), Collections.<DataPointListener>emptyList());
		QueryMetric metric = new QueryMetric(1L, 1, "metric1");

		QueryResults queryResults = datastore.query(metric);
		List<DataPointGroup> results = queryResults.getDataPoints();

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

		queryResults.close();
	}

	@SuppressWarnings({"ResultOfMethodCallIgnored", "ConstantConditions"})
	@Test
	public void test_cleanCacheDir() throws IOException, DatastoreException
	{
		TestDatastore testds = new TestDatastore();
		KairosDatastore datastore = new KairosDatastore(testds, new QueryQueuingManager(1, "hostname"), Collections.<DataPointListener>emptyList());

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

	private class TestDatastore implements Datastore
	{

		protected TestDatastore() throws DatastoreException
		{
		}

		@Override
		public void close() throws InterruptedException
		{
		}

		@Override
		public void putDataPoints(DataPointSet dps)
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

		@Override
		public List<DataPointRow> queryDatabase(DatastoreMetricQuery query, CachedSearchResult cachedSearchResult)
		{
			List<DataPointRow> groups = new ArrayList<DataPointRow>();

			TestingDataPointRowImpl group1 = new TestingDataPointRowImpl();
			group1.setName(query.getName());
			group1.addDataPoint(new DataPoint(1, 3));
			group1.addDataPoint(new DataPoint(1, 10));
			group1.addDataPoint(new DataPoint(1, 20));
			group1.addDataPoint(new DataPoint(2, 1));
			group1.addDataPoint(new DataPoint(2, 3));
			group1.addDataPoint(new DataPoint(2, 5));
			group1.addDataPoint(new DataPoint(3, 25));
			groups.add(group1);

			TestingDataPointRowImpl group2 = new TestingDataPointRowImpl();
			group2.setName(query.getName());
			group2.addDataPoint(new DataPoint(1, 5));
			group2.addDataPoint(new DataPoint(1, 14));
			group2.addDataPoint(new DataPoint(1, 20));
			group2.addDataPoint(new DataPoint(2, 6));
			group2.addDataPoint(new DataPoint(2, 8));
			group2.addDataPoint(new DataPoint(2, 9));
			group2.addDataPoint(new DataPoint(3, 7));
			groups.add(group2);

			return groups;
		}

		@Override
		public void deleteDataPoints(DatastoreMetricQuery deleteQuery, CachedSearchResult cachedSearchResult) throws DatastoreException
		{
		}
	}
}