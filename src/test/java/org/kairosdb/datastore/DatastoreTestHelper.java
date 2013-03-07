// KairosDB2
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

package org.kairosdb.datastore;

import com.google.common.collect.SetMultimap;
import com.google.common.collect.TreeMultimap;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.aggregator.AggregatorFactory;
import org.kairosdb.core.aggregator.TestAggregatorFactory;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.exception.DatastoreException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public abstract class DatastoreTestHelper
{
	protected AggregatorFactory m_aggFactory = new TestAggregatorFactory();
	protected static Datastore s_datastore;
	private static long s_startTime;

	private static List<String> listFromIterable(Iterable<String> iterable)
	{
		List<String> ret = new ArrayList<String>();
		for (String s : iterable)
		{
			ret.add(s);
		}

		return (ret);
	}

	private static SetMultimap<String, String> extractTags(DataPointGroup dpGroup)
	{
		SetMultimap<String, String> resp = TreeMultimap.create();

		for (String tag : dpGroup.getTagNames())
		{
			for (String value : dpGroup.getTagValues(tag))
			{
				resp.put(tag, value);
			}
		}

		return (resp);
	}

	/**
	 * Must be called from implementing class
	 */
	protected static void loadData() throws DatastoreException
	{
		DataPointSet dpSet = new DataPointSet("metric1");
		dpSet.addTag("host", "A");
		dpSet.addTag("client", "foo");
		dpSet.addTag("month", "April");

		s_startTime = System.currentTimeMillis();
		dpSet.addDataPoint(new DataPoint(s_startTime, 1));
		dpSet.addDataPoint(new DataPoint(s_startTime + 1000, 2));
		dpSet.addDataPoint(new DataPoint(s_startTime + 2000, 3));
		dpSet.addDataPoint(new DataPoint(s_startTime + 3000, 4));

		s_datastore.putDataPoints(dpSet);

		dpSet = new DataPointSet("metric1");
		dpSet.addTag("host", "B");
		dpSet.addTag("client", "foo");
		dpSet.addTag("month", "April");

		dpSet.addDataPoint(new DataPoint(s_startTime, 5));
		dpSet.addDataPoint(new DataPoint(s_startTime + 1000, 6));
		dpSet.addDataPoint(new DataPoint(s_startTime + 2000, 7));
		dpSet.addDataPoint(new DataPoint(s_startTime + 3000, 8));

		s_datastore.putDataPoints(dpSet);

		dpSet = new DataPointSet("metric1");
		dpSet.addTag("host", "A");
		dpSet.addTag("client", "bar");
		dpSet.addTag("month", "April");

		dpSet.addDataPoint(new DataPoint(s_startTime, 9));
		dpSet.addDataPoint(new DataPoint(s_startTime + 1000, 10));
		dpSet.addDataPoint(new DataPoint(s_startTime + 2000, 11));
		dpSet.addDataPoint(new DataPoint(s_startTime + 3000, 12));

		s_datastore.putDataPoints(dpSet);

		dpSet = new DataPointSet("metric2");
		dpSet.addTag("host", "B");
		dpSet.addTag("client", "bar");
		dpSet.addTag("month", "April");

		dpSet.addDataPoint(new DataPoint(s_startTime, 13));
		dpSet.addDataPoint(new DataPoint(s_startTime + 1000, 14));
		dpSet.addDataPoint(new DataPoint(s_startTime + 2000, 15));
		dpSet.addDataPoint(new DataPoint(s_startTime + 3000, 16));

		s_datastore.putDataPoints(dpSet);
	}

	@Test
	public void test_getMetricNames() throws DatastoreException
	{
		List<String> metrics = listFromIterable(s_datastore.getMetricNames());

		assertThat(metrics, hasItem("metric1"));
		assertThat(metrics, hasItem("metric2"));
	}

	@Test
	public void test_getTagNames() throws DatastoreException
	{
		List<String> metrics = listFromIterable(s_datastore.getTagNames());

		assertThat(metrics, hasItem("host"));
		assertThat(metrics, hasItem("client"));
		assertThat(metrics, hasItem("month"));
	}

	@Test
	public void test_getTagValues() throws DatastoreException
	{
		List<String> metrics = listFromIterable(s_datastore.getTagValues());

		assertThat(metrics, hasItem("A"));
		assertThat(metrics, hasItem("B"));
		assertThat(metrics, hasItem("foo"));
		assertThat(metrics, hasItem("bar"));
		assertThat(metrics, hasItem("April"));
	}

	@Test
	public void test_queryDatabase_noTags() throws DatastoreException
	{

		Map<String, String> tags = new TreeMap<String, String>();
		QueryMetric query = new QueryMetric(s_startTime, 0, "metric1");
		query.addAggregator(m_aggFactory.createAggregator("sort"));
		query.setEndTime(s_startTime + 3000);

		query.setTags(tags);

		List<DataPointGroup> results = s_datastore.query(query);

		assertThat(results.size(), equalTo(1));

		DataPointGroup dpg = results.get(0);

		assertThat(dpg.getName(), is("metric1"));
		SetMultimap<String, String> resTags = extractTags(dpg);

		assertThat(resTags.size(), is(5));
		SetMultimap<String, String> expectedTags = TreeMultimap.create();
		expectedTags.put("host", "A");
		expectedTags.put("host", "B");
		expectedTags.put("client", "foo");
		expectedTags.put("client", "bar");
		expectedTags.put("month", "April");

		assertThat(resTags, is(expectedTags));

		assertValues(dpg, 1, 5, 9, 2, 6, 10, 3, 7, 11, 4, 8, 12);

		dpg.close();
	}

	@Test
	public void test_queryDatabase_withTags() throws DatastoreException
	{

		Map<String, String> tags = new TreeMap<String, String>();
		tags.put("client", "foo");
		QueryMetric query = new QueryMetric(s_startTime, 0, "metric1");
		query.addAggregator(m_aggFactory.createAggregator("sort"));
		query.setEndTime(s_startTime + 3000);

		query.setTags(tags);

		List<DataPointGroup> results = s_datastore.query(query);

		assertThat(results.size(), is(1));

		DataPointGroup dpg = results.get(0);

		assertThat(dpg.getName(), is("metric1"));
		SetMultimap<String, String> resTags = extractTags(dpg);

		assertEquals(4, resTags.size());
		SetMultimap<String, String> expectedTags = TreeMultimap.create();
		expectedTags.put("host", "A");
		expectedTags.put("host", "B");
		expectedTags.put("client", "foo");
		expectedTags.put("month", "April");

		assertThat(resTags, is(expectedTags));

		assertValues(dpg, 1, 5, 2, 6, 3, 7, 4, 8);

		dpg.close();
	}

	@Test
	public void test_queryDatabase_withGroupBy() throws DatastoreException
	{

		Map<String, String> tags = new TreeMap<String, String>();
		QueryMetric query = new QueryMetric(s_startTime, 0, "metric1");
		query.addAggregator(m_aggFactory.createAggregator("sort"));
		query.setGroupBy("host");
		query.setEndTime(s_startTime + 3000);

		query.setTags(tags);

		List<DataPointGroup> results = s_datastore.query(query);

		assertThat(results.size(), is(2));

		DataPointGroup dpg = results.get(0);
		assertThat(dpg.getName(), is("metric1"));
		SetMultimap<String, String> resTags = extractTags(dpg);

		assertThat(resTags.keySet().size(), is(3));
		SetMultimap<String, String> expectedTags = TreeMultimap.create();
		expectedTags.put("host", "A");
		expectedTags.put("client", "foo");
		expectedTags.put("client", "bar");
		expectedTags.put("month", "April");

		assertThat(resTags, is(resTags));

		assertValues(dpg, 1, 9, 2, 10, 3, 11, 4, 12);

		dpg = results.get(1);
		assertThat(dpg.getName(), is("metric1"));
		resTags = extractTags(dpg);

		assertThat(resTags.keySet().size(), is(3));
		expectedTags = TreeMultimap.create();
		expectedTags.put("host", "B");
		expectedTags.put("client", "foo");
		expectedTags.put("month", "April");

		assertThat(resTags, is(resTags));

		assertValues(dpg, 5, 6, 7, 8);

		dpg.close();
	}

	private void assertValues(DataPointGroup group, long... values)
	{
		//System.out.println("start_time=" + s_startTime);
		for (long expected : values)
		{
			assertThat(group.hasNext(), is(true));
			long actual = group.next().getLongValue();
			assertThat(actual, is(expected));
		}

		assertThat(group.hasNext(), is(false));
	}
}
