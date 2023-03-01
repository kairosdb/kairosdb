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
package org.kairosdb.core.formatter;

import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.StringIterable;
import org.kairosdb.core.groupby.TagGroupBy;
import org.kairosdb.core.groupby.TagGroupByResult;
import org.kairosdb.metrics4j.collectors.LongCollector;
import org.kairosdb.testing.ListDataPointGroup;

import java.io.StringWriter;
import java.util.*;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class JsonFormatterTest
{

	@Test(expected = NullPointerException.class)
	public void test_format_nullWriterInvalid() throws FormatterException
	{
		JsonFormatter formatter = new JsonFormatter();

		formatter.format(null, new ArrayList<List<DataPointGroup>>());
	}

	@Test(expected = NullPointerException.class)
	public void test_format_nullDataInvalid() throws FormatterException
	{
		JsonFormatter formatter = new JsonFormatter();
		StringWriter writer = new StringWriter();

		formatter.format(writer, (List<List<DataPointGroup>>)null);
	}

	@Test
	public void test_format_counter() throws FormatterException
	{
		JsonFormatter formatter = new JsonFormatter();
		StringWriter writer = new StringWriter();
		List<String> list = new ArrayList<>();
		list.add("one");
		list.add("two");
		list.add("three");
		list.add("four");
		list.add("five");
		LongCollector collector = mock(LongCollector.class);

		formatter.format(writer, list, collector);

		verify(collector).put(5);
	}

	@Test
	public void test_format() throws FormatterException
	{
		List<DataPointGroup> metric1 = new ArrayList<DataPointGroup>();

		ListDataPointGroup group1 = new ListDataPointGroup("group1");
		group1.addTag("tag1", "tag1Value");
		group1.addTag("tag2", "tag2Value");
		group1.addDataPoint(new LongDataPoint(1, 10));
		group1.addDataPoint(new LongDataPoint(1, 20));
		group1.addDataPoint(new LongDataPoint(1, 3));
		group1.addDataPoint(new LongDataPoint(2, 1));
		group1.addDataPoint(new LongDataPoint(2, 3));
		group1.addDataPoint(new LongDataPoint(2, 5));
		group1.addDataPoint(new LongDataPoint(3, 25));
		metric1.add(group1);

		List<DataPointGroup> metric2 = new ArrayList<DataPointGroup>();

		ListDataPointGroup group2 = new ListDataPointGroup("group2");
		group2.addTag("tag3", "tag3Value");
		group2.addTag("tag4", "tag4Value");
		group2.addDataPoint(new LongDataPoint(1, 5));
		group2.addDataPoint(new LongDataPoint(1, 20));
		group2.addDataPoint(new LongDataPoint(1, 14));
		group2.addDataPoint(new LongDataPoint(2, 6));
		group2.addDataPoint(new LongDataPoint(2, 9));
		group2.addDataPoint(new LongDataPoint(2, 8));
		group2.addDataPoint(new LongDataPoint(3, 7));
		metric2.add(group2);

		List<List<DataPointGroup>> results = new ArrayList<List<DataPointGroup>>();
		results.add(metric1);
		results.add(metric2);

		JsonFormatter formatter = new JsonFormatter();
		StringWriter writer = new StringWriter();
		formatter.format(writer, results);

		assertThat(writer.toString(),
				equalTo("{\"queries\":[{\"results\":[{\"name\":\"group1\",\"tags\":{\"tag1\":[\"tag1Value\"],\"tag2\":[\"tag2Value\"]},\"values\":[[1,10],[1,20],[1,3],[2,1],[2,3],[2,5],[3,25]]}]},{\"results\":[{\"name\":\"group2\",\"tags\":{\"tag3\":[\"tag3Value\"],\"tag4\":[\"tag4Value\"]},\"values\":[[1,5],[1,20],[1,14],[2,6],[2,9],[2,8],[3,7]]}]}]}"));

	}

	@Test
	public void test_format_withGroupBy() throws FormatterException
	{
		List<DataPointGroup> metric1 = new ArrayList<DataPointGroup>();

		ListDataPointGroup group1 = new ListDataPointGroup("group1");
		group1.addTag("tag1", "tag1Value");
		group1.addTag("tag2", "tag2Value");

		Map<String, String> tagResults = new LinkedHashMap<String, String>();
		tagResults.put("foo", "fi");
		tagResults.put("bar", "fo");
		group1.addGroupByResult(new TagGroupByResult(new TagGroupBy("foo", "bar"), tagResults));

		group1.addDataPoint(new LongDataPoint(1, 10));
		group1.addDataPoint(new LongDataPoint(1, 20));
		group1.addDataPoint(new LongDataPoint(1, 3));
		group1.addDataPoint(new LongDataPoint(2, 1));
		group1.addDataPoint(new LongDataPoint(2, 3));
		group1.addDataPoint(new LongDataPoint(2, 5));
		group1.addDataPoint(new LongDataPoint(3, 25));
		metric1.add(group1);

		List<List<DataPointGroup>> results = new ArrayList<List<DataPointGroup>>();
		results.add(metric1);

		JsonFormatter formatter = new JsonFormatter();
		StringWriter writer = new StringWriter();
		formatter.format(writer, results);

		assertThat(writer.toString(),
				equalTo("{\"queries\":[{\"results\":[{\"name\":\"group1\",\"group_by\":[{\"name\":\"tag\",\"tags\":[\"foo\",\"bar\"],\"group\":{\"foo\":\"fi\",\"bar\":\"fo\"}}],\"tags\":{\"tag1\":[\"tag1Value\"],\"tag2\":[\"tag2Value\"]},\"values\":[[1,10],[1,20],[1,3],[2,1],[2,3],[2,5],[3,25]]}]}]}"));

	}

	@Test(expected = NullPointerException.class)
	public void test_formatStringIterable_nullWriterInvalid() throws FormatterException
	{
		JsonFormatter formatter = new JsonFormatter();

		formatter.format(null, new TestStringIterable(), null);
	}

	@Test(expected = NullPointerException.class)
	public void test_formatStringIterable_nullIterableInvalid() throws FormatterException
	{
		JsonFormatter formatter = new JsonFormatter();
		StringWriter writer = new StringWriter();

		formatter.format(writer, (StringIterable)null, null);
	}

	@Test
	public void test_formatStringIterable() throws FormatterException
	{
		JsonFormatter formatter = new JsonFormatter();
		StringWriter writer = new StringWriter();
		formatter.format(writer, new TestStringIterable(), null);

		assertThat(writer.toString(), equalTo("{\"results\":[\"Phil\",\"Bob\",\"Larry\",\"Moe\",\"Curly\"]}"));
	}

	private class TestStringIterable extends StringIterable
	{

		@Override
		public Iterator<String> iterator()
		{
			List<String> list = new ArrayList<String>();
			list.add("Phil");
			list.add("Bob");
			list.add("Larry");
			list.add("Moe");
			list.add("Curly");
			return list.iterator();
		}
	}


}