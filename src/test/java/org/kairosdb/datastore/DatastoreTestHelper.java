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

package org.kairosdb.datastore;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.TreeMultimap;
import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.DatastoreQuery;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.groupby.TagGroupBy;

import java.util.*;

import static junit.framework.Assert.assertFalse;
import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public abstract class DatastoreTestHelper
{
	protected static KairosDatastore s_datastore;
	protected static final List<String> metricNames = new ArrayList<String>();
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
		metricNames.add("metric1");
		DataPointSet dpSet = new DataPointSet("metric1");
		dpSet.addTag("host", "A");
		dpSet.addTag("client", "foo");
		dpSet.addTag("month", "April");

		s_startTime = System.currentTimeMillis();
		dpSet.addDataPoint(new LongDataPoint(s_startTime, 1));
		dpSet.addDataPoint(new LongDataPoint(s_startTime + 1000, 2));
		dpSet.addDataPoint(new LongDataPoint(s_startTime + 2000, 3));
		dpSet.addDataPoint(new LongDataPoint(s_startTime + 3000, 4));

		s_datastore.putDataPoints(dpSet);

		dpSet = new DataPointSet("metric1");
		dpSet.addTag("host", "B");
		dpSet.addTag("client", "foo");
		dpSet.addTag("month", "April");

		dpSet.addDataPoint(new LongDataPoint(s_startTime, 5));
		dpSet.addDataPoint(new LongDataPoint(s_startTime + 1000, 6));
		dpSet.addDataPoint(new LongDataPoint(s_startTime + 2000, 7));
		dpSet.addDataPoint(new LongDataPoint(s_startTime + 3000, 8));

		s_datastore.putDataPoints(dpSet);

		dpSet = new DataPointSet("metric1");
		dpSet.addTag("host", "A");
		dpSet.addTag("client", "bar");
		dpSet.addTag("month", "April");

		dpSet.addDataPoint(new LongDataPoint(s_startTime, 9));
		dpSet.addDataPoint(new LongDataPoint(s_startTime + 1000, 10));
		dpSet.addDataPoint(new LongDataPoint(s_startTime + 2000, 11));
		dpSet.addDataPoint(new LongDataPoint(s_startTime + 3000, 12));

		s_datastore.putDataPoints(dpSet);

		metricNames.add("metric2");
		dpSet = new DataPointSet("metric2");
		dpSet.addTag("host", "B");
		dpSet.addTag("client", "bar");
		dpSet.addTag("month", "April");

		dpSet.addDataPoint(new LongDataPoint(s_startTime, 13));
		dpSet.addDataPoint(new LongDataPoint(s_startTime + 1000, 14));
		dpSet.addDataPoint(new LongDataPoint(s_startTime + 2000, 15));
		dpSet.addDataPoint(new LongDataPoint(s_startTime + 3000, 16));

		s_datastore.putDataPoints(dpSet);

		metricNames.add("duplicates");
		dpSet = new DataPointSet("duplicates");
		dpSet.addTag("host", "A");
		dpSet.addDataPoint(new LongDataPoint(s_startTime, 4));

		s_datastore.putDataPoints(dpSet);

		dpSet = new DataPointSet("duplicates");
		dpSet.addTag("host", "A");
		dpSet.addDataPoint(new LongDataPoint(s_startTime, 42));

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
		query.setEndTime(s_startTime + 3000);

		query.setTags(tags);

		DatastoreQuery dq = s_datastore.createQuery(query);

		List<DataPointGroup> results = dq.execute();

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

		dq.close();
	}

	@Test
	public void test_queryDatabase_withTags() throws DatastoreException
	{

		Map<String, String> tags = new TreeMap<String, String>();
		tags.put("client", "foo");
		QueryMetric query = new QueryMetric(s_startTime, 0, "metric1");
		query.setEndTime(s_startTime + 3000);

		query.setTags(tags);

		DatastoreQuery dq = s_datastore.createQuery(query);
		List<DataPointGroup> results = dq.execute();

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

		dq.close();
	}

	@Test
	public void test_queryDatabase_withTagGroupBy() throws DatastoreException
	{
		Map<String, String> tags = new TreeMap<String, String>();
		QueryMetric query = new QueryMetric(s_startTime, 0, "metric1");
		query.addGroupBy(new TagGroupBy(Collections.singletonList("host")));
		query.setEndTime(s_startTime + 3000);

		query.setTags(tags);

		DatastoreQuery dq = s_datastore.createQuery(query);
		List<DataPointGroup> results = dq.execute();

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

		dq.close();
	}

	@Test
	public void test_queryDatabase_withMultipleTagGroupBy() throws DatastoreException
	{
		Map<String, String> tags = new TreeMap<String, String>();
		QueryMetric query = new QueryMetric(s_startTime, 0, "metric1");
		query.addGroupBy(new TagGroupBy("host", "client"));
		query.setEndTime(s_startTime + 3000);

		query.setTags(tags);

		DatastoreQuery dq = s_datastore.createQuery(query);
		List<DataPointGroup> results = dq.execute();

		assertThat(results.size(), is(3));

		DataPointGroup dpg = results.get(0);
		assertThat(dpg.getName(), is("metric1"));
		SetMultimap<String, String> resTags = extractTags(dpg);

		assertThat(resTags.keySet().size(), is(3));
		SetMultimap<String, String> expectedTags = TreeMultimap.create();
		expectedTags.put("host", "A");
		expectedTags.put("client", "foo");
		expectedTags.put("month", "April");
		assertThat(resTags, is(resTags));
		assertValues(dpg, 1, 2, 3, 4);

		dpg = results.get(1);
		assertThat(dpg.getName(), is("metric1"));
		resTags = extractTags(dpg);

		assertThat(resTags.keySet().size(), is(3));
		expectedTags = TreeMultimap.create();
		expectedTags.put("host", "A");
		expectedTags.put("client", "bar");
		expectedTags.put("month", "April");

		assertThat(resTags, is(resTags));

		assertValues(dpg, 9, 10, 11, 12);

		dpg = results.get(2);
		assertThat(dpg.getName(), is("metric1"));
		resTags = extractTags(dpg);

		assertThat(resTags.keySet().size(), is(3));
		expectedTags = TreeMultimap.create();
		expectedTags.put("host", "B");
		expectedTags.put("client", "foo");
		expectedTags.put("month", "April");

		assertThat(resTags, is(resTags));

		assertValues(dpg, 5, 6, 7, 8);

		dq.close();
	}

	@Test
	public void test_queryDatabase_withGroupBy_nonMatchingTag() throws DatastoreException
	{
		Map<String, String> tags = new TreeMap<String, String>();
		QueryMetric query = new QueryMetric(s_startTime, 0, "metric1");
		query.addGroupBy(new TagGroupBy("bogus"));
		query.setEndTime(s_startTime + 3000);

		query.setTags(tags);

		DatastoreQuery dq = s_datastore.createQuery(query);
		List<DataPointGroup> results = dq.execute();

		assertThat(results.size(), is(1));

		DataPointGroup dpg = results.get(0);
		assertThat(dpg.getName(), is("metric1"));
		SetMultimap<String, String> resTags = extractTags(dpg);

		assertThat(resTags.keySet().size(), is(3));
		SetMultimap<String, String> expectedTags = TreeMultimap.create();
		expectedTags.put("host", "A");
		expectedTags.put("host", "B");
		expectedTags.put("client", "foo");
		expectedTags.put("client", "bar");
		expectedTags.put("month", "April");
		assertThat(resTags, is(resTags));
		assertValues(dpg, 1, 5, 9, 2, 6, 10, 3, 7, 11, 4, 8, 12);

		dq.close();
	}

	@Test
	public void test_queryWithMultipleTagsFilter() throws DatastoreException
	{
		Map<String, String> tags = new TreeMap<String, String>();
		tags.put("host", "A");
		tags.put("client", "bar");
		QueryMetric query = new QueryMetric(s_startTime, 0, "metric1");
		query.addGroupBy(new TagGroupBy(Collections.singletonList("host")));
		query.setEndTime(s_startTime + 3000);

		query.setTags(tags);

		DatastoreQuery dq = s_datastore.createQuery(query);
		List<DataPointGroup> results = dq.execute();

		assertThat(results.size(), is(1));

		assertValues(results.get(0), 9, 10, 11, 12);

		dq.close();
	}

	@Test
	public void test_queryWithMultipleHostTags() throws DatastoreException
	{
		SetMultimap<String, String> tags = HashMultimap.create();
		QueryMetric query = new QueryMetric(s_startTime, 0, "metric1");
		query.setEndTime(s_startTime + 3000);

		tags.put("host", "A");
		tags.put("host", "B");
		query.setTags(tags);

		DatastoreQuery dq = s_datastore.createQuery(query);
		List<DataPointGroup> results = dq.execute();

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

		dq.close();
	}

	@Test
	public void test_duplicateDataPoints() throws DatastoreException
	{
		SetMultimap<String, String> tags = HashMultimap.create();
		QueryMetric query = new QueryMetric(s_startTime, 0, "duplicates");
		query.setEndTime(s_startTime + 3000);

		query.setTags(tags);

		DatastoreQuery dq = s_datastore.createQuery(query);
		List<DataPointGroup> results = dq.execute();

		assertThat(results.size(), equalTo(1));

		DataPointGroup dpg = results.get(0);

		assertThat(dpg.getName(), is("duplicates"));
		SetMultimap<String, String> resTags = extractTags(dpg);

		assertThat(resTags.size(), is(1));
		SetMultimap<String, String> expectedTags = TreeMultimap.create();
		expectedTags.put("host", "A");

		assertThat(resTags, is(expectedTags));

		assertValues(dpg, 42);

		dq.close();
	}

	@Test
	public void test_queryDatabase_noResults() throws DatastoreException
	{

		Map<String, String> tags = new TreeMap<String, String>();
		QueryMetric query = new QueryMetric(500, 0, "metric1");
		query.setEndTime(1000);

		query.setTags(tags);

		DatastoreQuery dq = s_datastore.createQuery(query);

		List<DataPointGroup> results = dq.execute();

		assertThat(results.size(), equalTo(1));
		DataPointGroup dpg = results.get(0);
		assertThat(dpg.getName(), is("metric1"));
		assertFalse(dpg.hasNext());

		dq.close();
	}


	private void assertValues(DataPointGroup group, long... values)
	{
		for (long expected : values)
		{
			assertThat(group.hasNext(), is(true));
			long actual = group.next().getLongValue();
			assertThat(actual, is(expected));
		}

		assertThat(group.hasNext(), is(false));
	}
}
