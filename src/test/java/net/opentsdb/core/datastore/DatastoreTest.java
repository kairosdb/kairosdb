// OpenTSDB2
// Copyright (C) 2013 Proofpoint, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>
package net.opentsdb.core.datastore;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPointSet;
import net.opentsdb.core.exception.DatastoreException;
import net.opentsdb.core.exception.TsdbException;
import net.opentsdb.core.exception.UnknownAggregator;
import net.opentsdb.testing.TestingDataPointRowImpl;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class DatastoreTest
{

	@Test(expected = NullPointerException.class)
	public void test_query_nullMetricInvalid() throws TsdbException
	{
		TestDatastore datastore = new TestDatastore();

		datastore.query(null);
	}

	@Test(expected = UnknownAggregator.class)
	public void test_query_invalidAggregator() throws TsdbException
	{
		TestDatastore datastore = new TestDatastore();
		QueryMetric metric = new QueryMetric(1L, 1, "metric1", "bogus");

		datastore.query(metric);
	}

	@Test
	public void test_query_sumAggregator() throws TsdbException
	{
		TestDatastore datastore = new TestDatastore();
		QueryMetric metric = new QueryMetric(1L, 1, "metric1", "sum");

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
		QueryMetric metric = new QueryMetric(1L, 1, "metric1", "none");

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
			group1.addDataPoint(new DataPoint(1, 3));
			group1.addDataPoint(new DataPoint(1, 10));
			group1.addDataPoint(new DataPoint(1, 20));
			group1.addDataPoint(new DataPoint(2, 1));
			group1.addDataPoint(new DataPoint(2, 3));
			group1.addDataPoint(new DataPoint(2, 5));
			group1.addDataPoint(new DataPoint(3, 25));
			groups.add(group1);

			TestingDataPointRowImpl group2 = new TestingDataPointRowImpl();
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