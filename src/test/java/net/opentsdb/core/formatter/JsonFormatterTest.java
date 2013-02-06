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
package net.opentsdb.core.formatter;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.datastore.DataPointGroup;
import net.opentsdb.core.datastore.StringIterable;
import net.opentsdb.testing.ListDataPointGroup;
import org.junit.Test;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

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
	public void test_format() throws FormatterException
	{
		List<DataPointGroup> metric1 = new ArrayList<DataPointGroup>();

		ListDataPointGroup group1 = new ListDataPointGroup("group1");
		group1.addTag("tag1", "tag1Value");
		group1.addTag("tag2", "tag2Value");
		group1.addDataPoint(new DataPoint(1, 10));
		group1.addDataPoint(new DataPoint(1, 20));
		group1.addDataPoint(new DataPoint(1, 3));
		group1.addDataPoint(new DataPoint(2, 1));
		group1.addDataPoint(new DataPoint(2, 3));
		group1.addDataPoint(new DataPoint(2, 5));
		group1.addDataPoint(new DataPoint(3, 25));
		metric1.add(group1);

		List<DataPointGroup> metric2 = new ArrayList<DataPointGroup>();

		ListDataPointGroup group2 = new ListDataPointGroup("group2");
		group2.addTag("tag3", "tag3Value");
		group2.addTag("tag4", "tag4Value");
		group2.addDataPoint(new DataPoint(1, 5));
		group2.addDataPoint(new DataPoint(1, 20));
		group2.addDataPoint(new DataPoint(1, 14));
		group2.addDataPoint(new DataPoint(2, 6));
		group2.addDataPoint(new DataPoint(2, 9));
		group2.addDataPoint(new DataPoint(2, 8));
		group2.addDataPoint(new DataPoint(3, 7));
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

	@Test(expected = NullPointerException.class)
	public void test_formatStringIterable_nullWriterInvalid() throws FormatterException
	{
		JsonFormatter formatter = new JsonFormatter();

		formatter.format(null, new TestStringIterable());
	}

	@Test(expected = NullPointerException.class)
	public void test_formatStringIterable_nullIterableInvalid() throws FormatterException
	{
		JsonFormatter formatter = new JsonFormatter();
		StringWriter writer = new StringWriter();

		formatter.format(writer, (StringIterable)null);
	}

	@Test
	public void test_formatStringIterable() throws FormatterException
	{
		JsonFormatter formatter = new JsonFormatter();
		StringWriter writer = new StringWriter();
		formatter.format(writer, new TestStringIterable());

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