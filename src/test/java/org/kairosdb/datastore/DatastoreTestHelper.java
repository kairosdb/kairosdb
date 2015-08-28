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
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.TreeMultimap;
import junit.framework.TestCase;
import org.hamcrest.CoreMatchers;
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
import static junit.framework.Assert.assertTrue;
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
	private static String s_unicodeNameWithSpace = "你好 means hello";
	private static String s_unicodeName = "你好";

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
		ImmutableSortedMap<String, String> tags;
		String metricName = "metric1";
		tags = ImmutableSortedMap.<String, String>naturalOrder()
				.put("host", "A")
				.put("client", "foo")
				.put("month", "April")
				.build();

		s_startTime = System.currentTimeMillis();
		s_datastore.putDataPoint(metricName, tags, new LongDataPoint(s_startTime, 1));
		s_datastore.putDataPoint(metricName, tags, new LongDataPoint(s_startTime + 1000, 2));
		s_datastore.putDataPoint(metricName, tags, new LongDataPoint(s_startTime + 2000, 3));
		s_datastore.putDataPoint(metricName, tags, new LongDataPoint(s_startTime + 3000, 4));


		tags = ImmutableSortedMap.<String, String>naturalOrder()
				.put("host", "B")
				.put("client", "foo")
				.put("month", "April")
				.build();

		s_datastore.putDataPoint(metricName, tags, new LongDataPoint(s_startTime, 5));
		s_datastore.putDataPoint(metricName, tags, new LongDataPoint(s_startTime + 1000, 6));
		s_datastore.putDataPoint(metricName, tags, new LongDataPoint(s_startTime + 2000, 7));
		s_datastore.putDataPoint(metricName, tags, new LongDataPoint(s_startTime + 3000, 8));


		tags = ImmutableSortedMap.<String, String>naturalOrder()
				.put("host", "A")
				.put("client", "bar")
				.put("month", "April")
				.build();

		s_datastore.putDataPoint(metricName, tags, new LongDataPoint(s_startTime, 9));
		s_datastore.putDataPoint(metricName, tags, new LongDataPoint(s_startTime + 1000, 10));
		s_datastore.putDataPoint(metricName, tags, new LongDataPoint(s_startTime + 2000, 11));
		s_datastore.putDataPoint(metricName, tags, new LongDataPoint(s_startTime + 3000, 12));


		metricNames.add("metric2");
		metricName = "metric2";
		tags = ImmutableSortedMap.<String, String>naturalOrder()
				.put("host", "B")
				.put("client", "bar")
				.put("month", "April")
				.build();

		s_datastore.putDataPoint(metricName, tags, new LongDataPoint(s_startTime, 13));
		s_datastore.putDataPoint(metricName, tags, new LongDataPoint(s_startTime + 1000, 14));
		s_datastore.putDataPoint(metricName, tags, new LongDataPoint(s_startTime + 2000, 15));
		s_datastore.putDataPoint(metricName, tags, new LongDataPoint(s_startTime + 3000, 16));


		metricNames.add("duplicates");
		metricName = "duplicates";
		tags = ImmutableSortedMap.<String, String>naturalOrder()
				.put("host", "A")
				.build();

		s_datastore.putDataPoint(metricName, tags, new LongDataPoint(s_startTime, 4));

		s_datastore.putDataPoint(metricName, tags, new LongDataPoint(s_startTime, 42));


		//Testing pre 1970 data points with negative values
		metricNames.add("old_data");
		metricName = "old_data";
		tags = ImmutableSortedMap.<String, String>naturalOrder()
				.put("host", "A").build();

		s_datastore.putDataPoint(metricName, tags, new LongDataPoint(-2000000000L, 80));
		s_datastore.putDataPoint(metricName, tags, new LongDataPoint(-1000000000L, 40));
		s_datastore.putDataPoint(metricName, tags, new LongDataPoint(-100L, 20));
		s_datastore.putDataPoint(metricName, tags, new LongDataPoint(0L, 3));
		s_datastore.putDataPoint(metricName, tags, new LongDataPoint(2000000000L, 33));


		//Adding a metric with unicode and spaces
		metricNames.add(s_unicodeNameWithSpace);
		metricName = s_unicodeNameWithSpace;
		tags = ImmutableSortedMap.<String, String>naturalOrder()
				.put("host", s_unicodeName)
				.put("space", "space is cool").build();

		s_datastore.putDataPoint(metricName, tags, new LongDataPoint(s_startTime, 42));


		//Data that will be deleted in test
		metricName = "delete_me";
		metricNames.add(metricName);
		tags = ImmutableSortedMap.<String, String>naturalOrder()
				.put("ghost", "tag").build();

		s_datastore.putDataPoint(metricName, tags, new LongDataPoint(s_startTime, 50));
	}

	@Test
	public void test_getMetricNames() throws DatastoreException
	{
		List<String> metrics = listFromIterable(s_datastore.getMetricNames());

		assertThat(metrics, hasItem("metric1"));
		assertThat(metrics, hasItem("metric2"));
		assertThat(metrics, hasItem("duplicates"));
		assertThat(metrics, hasItem("old_data"));
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

		try
		{
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
		}
		finally
		{
			dq.close();
		}
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
		try
		{
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
		}
		finally
		{
			dq.close();
		}
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
		try
		{
			List<DataPointGroup> results = dq.execute();

			assertThat(results.size(), is(2));

			DataPointGroup dpg = results.get(0);
			assertThat(dpg.getName(), is("metric1"));
			SetMultimap<String, String> resTags = extractTags(dpg);

			SetMultimap<String, String> expectedTags = TreeMultimap.create();
			expectedTags.put("host", "A");
			expectedTags.put("client", "foo");
			expectedTags.put("client", "bar");
			expectedTags.put("month", "April");

			assertThat(resTags, equalTo(expectedTags));
			assertValues(dpg, 1, 9, 2, 10, 3, 11, 4, 12);


			dpg = results.get(1);
			assertThat(dpg.getName(), is("metric1"));
			resTags = extractTags(dpg);

			expectedTags = TreeMultimap.create();
			expectedTags.put("host", "B");
			expectedTags.put("client", "foo");
			expectedTags.put("month", "April");

			assertThat(resTags, equalTo(expectedTags));
			assertValues(dpg, 5, 6, 7, 8);
		}
		finally
		{
			dq.close();
		}
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
		try
		{
			List<DataPointGroup> results = dq.execute();

			assertThat(results.size(), is(3));

			DataPointGroup dpg = results.get(0);
			assertThat(dpg.getName(), is("metric1"));

			SetMultimap<String, String> resTags = extractTags(dpg);
			assertThat(resTags.keySet().size(), is(3));

			SetMultimap<String, String> expectedTags = TreeMultimap.create();
			expectedTags.put("host", "A");
			expectedTags.put("client", "bar");
			expectedTags.put("month", "April");

			assertThat(resTags, equalTo(expectedTags));
			assertValues(dpg, 9, 10, 11, 12);


			dpg = results.get(1);
			assertThat(dpg.getName(), is("metric1"));

			resTags = extractTags(dpg);
			assertThat(resTags.keySet().size(), is(3));

			expectedTags = TreeMultimap.create();
			expectedTags.put("host", "A");
			expectedTags.put("client", "foo");
			expectedTags.put("month", "April");

			assertThat(resTags, equalTo(expectedTags));
			assertValues(dpg, 1, 2, 3, 4);


			dpg = results.get(2);
			assertThat(dpg.getName(), is("metric1"));

			resTags = extractTags(dpg);
			assertThat(resTags.keySet().size(), is(3));

			expectedTags = TreeMultimap.create();
			expectedTags.put("host", "B");
			expectedTags.put("client", "foo");
			expectedTags.put("month", "April");

			assertThat(resTags, equalTo(expectedTags));
			assertValues(dpg, 5, 6, 7, 8);

		}
		finally
		{
			dq.close();
		}
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
		try
		{
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
			assertThat(resTags, is(expectedTags));
			assertValues(dpg, 1, 5, 9, 2, 6, 10, 3, 7, 11, 4, 8, 12);
		}
		finally
		{
			dq.close();
		}
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
		try
		{
			List<DataPointGroup> results = dq.execute();

			assertThat(results.size(), is(1));

			assertValues(results.get(0), 9, 10, 11, 12);
		}
		finally
		{
			dq.close();
		}
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
		try
		{
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
		}
		finally
		{
			dq.close();
		}
	}

	@Test
	public void test_duplicateDataPoints() throws DatastoreException
	{
		SetMultimap<String, String> tags = HashMultimap.create();
		QueryMetric query = new QueryMetric(s_startTime, 0, "duplicates");
		query.setEndTime(s_startTime + 3000);

		query.setTags(tags);

		DatastoreQuery dq = s_datastore.createQuery(query);
		try
		{
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
		}
		finally
		{
			dq.close();
		}
	}

	@Test
	public void test_queryDatabase_noResults() throws DatastoreException
	{

		Map<String, String> tags = new TreeMap<String, String>();
		QueryMetric query = new QueryMetric(500, 0, "metric1");
		query.setEndTime(1000);

		query.setTags(tags);

		DatastoreQuery dq = s_datastore.createQuery(query);
		try
		{
			List<DataPointGroup> results = dq.execute();

			assertThat(results.size(), equalTo(1));
			DataPointGroup dpg = results.get(0);
			assertThat(dpg.getName(), is("metric1"));
			assertFalse(dpg.hasNext());
		}
		finally
		{
			dq.close();
		}
	}

	@Test
	public void test_queryNegativeAndPositiveTime() throws DatastoreException
	{
		SetMultimap<String, String> tags = HashMultimap.create();
		QueryMetric query = new QueryMetric(-2000000000L, 0, "old_data");
		query.setEndTime(2000000000L);

		DatastoreQuery dq = s_datastore.createQuery(query);
		try
		{
			List<DataPointGroup> results = dq.execute();

			assertThat(results.size(), equalTo(1));

			DataPointGroup dpg = results.get(0);

			assertThat(dpg.getName(), is("old_data"));
			SetMultimap<String, String> resTags = extractTags(dpg);

			assertThat(resTags.size(), is(1));
			SetMultimap<String, String> expectedTags = TreeMultimap.create();
			expectedTags.put("host", "A");

			assertThat(resTags, is(expectedTags));

			/*while (dpg.hasNext())
				System.out.println(dpg.next());*/

			assertValues(dpg, 80, 40, 20, 3, 33);
		}
		finally
		{
			dq.close();
		}
	}
	
	@Test
	public void test_queryNegativeTime() throws DatastoreException
	{
		SetMultimap<String, String> tags = HashMultimap.create();
		QueryMetric query = new QueryMetric(-2000000000L, 0, "old_data");
		query.setEndTime(-1L);

		DatastoreQuery dq = s_datastore.createQuery(query);
		try
		{
			List<DataPointGroup> results = dq.execute();

			assertThat(results.size(), equalTo(1));

			DataPointGroup dpg = results.get(0);

			assertThat(dpg.getName(), is("old_data"));
			SetMultimap<String, String> resTags = extractTags(dpg);

			assertThat(resTags.size(), is(1));
			SetMultimap<String, String> expectedTags = TreeMultimap.create();
			expectedTags.put("host", "A");

			assertThat(resTags, is(expectedTags));

			assertValues(dpg, 80, 40, 20);
		}
		finally
		{
			dq.close();
		}
	}

	@Test
	public void test_queryWithUnicode() throws DatastoreException
	{
		SetMultimap<String, String> tags = HashMultimap.create();
		QueryMetric query = new QueryMetric(s_startTime, 0, s_unicodeNameWithSpace);
		query.setEndTime(s_startTime + 3000);

		tags.put("host", s_unicodeName);
		query.setTags(tags);

		DatastoreQuery dq = s_datastore.createQuery(query);
		try
		{
			List<DataPointGroup> results = dq.execute();

			assertThat(results.size(), equalTo(1));

			DataPointGroup dpg = results.get(0);

			assertThat(dpg.getName(), is(s_unicodeNameWithSpace));
			SetMultimap<String, String> resTags = extractTags(dpg);

			assertThat(resTags.size(), is(2));
			SetMultimap<String, String> expectedTags = TreeMultimap.create();
			expectedTags.put("host", s_unicodeName);
			expectedTags.put("space", "space is cool");

			assertThat(resTags, is(expectedTags));

			assertValues(dpg, 42);
		}
		finally
		{
			dq.close();
		}
	}

	@Test
	public void test_notReturningTagsForEmptyData() throws DatastoreException, InterruptedException
	{
		QueryMetric query = new QueryMetric(s_startTime -1, 0, "delete_me");
		query.setEndTime(s_startTime + 1);

		s_datastore.delete(query);

		Thread.sleep(1500);
		//Now query for the data
		DatastoreQuery dq = s_datastore.createQuery(query);
		try
		{
			List<DataPointGroup> results = dq.execute();

			assertThat(results.size(), equalTo(1));

			DataPointGroup dpg = results.get(0);
			SetMultimap<String, String> resTags = extractTags(dpg);
			assertThat(resTags.size(), is(0));

			assertThat(dpg.hasNext(), is(false));
		}
		finally
		{
			dq.close();
		}
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
