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

import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.aggregator.AggregatorFactory;
import org.kairosdb.core.aggregator.TestAggregatorFactory;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.exception.TsdbException;
import org.kairosdb.testing.TestingDataPointRowImpl;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class DatastoreTest
{
	private AggregatorFactory aggFactory = new TestAggregatorFactory();

	@Test(expected = NullPointerException.class)
	public void test_query_nullMetricInvalid() throws TsdbException
	{
		TestDatastore datastore = new TestDatastore();

		datastore.query(null);
	}

	/*@Test(expected = UnknownAggregator.class)
	public void test_query_invalidAggregator() throws TsdbException
	{
		TestDatastore datastore = new TestDatastore();
		QueryMetric metric = new QueryMetric(1L, 1, "metric1", "bogus");

		datastore.query(metric);
	}*/

	@Test
	public void test_query_sumAggregator() throws TsdbException
	{
		TestDatastore datastore = new TestDatastore();
		QueryMetric metric = new QueryMetric(1L, 1, "metric1");
		metric.addAggregator(aggFactory.createAggregator("sum"));

		List<DataPointGroup> results = datastore.query(metric);

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
	}

	@Test
	public void test_query_noneAggregator() throws TsdbException
	{
		TestDatastore datastore = new TestDatastore();
		QueryMetric metric = new QueryMetric(1L, 1, "metric1");
		metric.addAggregator(aggFactory.createAggregator("sort"));

		List<DataPointGroup> results = datastore.query(metric);

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
	}

	private class TestDatastore extends Datastore
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
		protected List<DataPointRow> queryDatabase(DatastoreMetricQuery query, CachedSearchResult cachedSearchResult)
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


	}
}