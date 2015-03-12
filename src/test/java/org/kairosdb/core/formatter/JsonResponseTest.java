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
package org.kairosdb.core.formatter;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datapoints.DoubleDataPoint;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.groupby.ValueGroupBy;
import org.kairosdb.testing.ListDataPointGroup;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class JsonResponseTest
{
	JsonParser parser = new JsonParser();
	private StringWriter writer;
	private JsonResponse response;

	@Before
	public void setup()
	{
		writer = new StringWriter();
		response = new JsonResponse(writer);
	}

	@Test
	public void test_two_groups() throws IOException, FormatterException
	{
		String json = Resources.toString(Resources.getResource("query-response-two-groups-valid.json"), Charsets.UTF_8);

		ValueGroupBy groupBy = new ValueGroupBy(10);
		List<DataPointGroup> groups = new ArrayList<DataPointGroup>();

		ListDataPointGroup group1 = new ListDataPointGroup("metric1");
		group1.addTag("tag1", "value1");
		group1.addTag("tag2", "value2");
		group1.addGroupByResult(groupBy.getGroupByResult(0));
		group1.addDataPoint(new LongDataPoint(12345, 1));
		group1.addDataPoint(new LongDataPoint(56789, 2));
		group1.addDataPoint(new DoubleDataPoint(98765, 2.9));

		ListDataPointGroup group2 = new ListDataPointGroup("metric1");
		group2.addTag("tag3", "value3");
		group2.addTag("tag4", "value4");
		group2.addGroupByResult(groupBy.getGroupByResult(1));
		group2.addDataPoint(new LongDataPoint(12345, 5));
		group2.addDataPoint(new LongDataPoint(56789, 6));
		group2.addDataPoint(new DoubleDataPoint(98765, 7.9));

		groups.add(group1);
		groups.add(group2);
                
                JsonObject queryObject = new JsonObject();
                queryObject.addProperty("name", "metric1");
                queryObject.add("tags", new JsonObject());
                // groupBy
                JsonArray groupBys = new JsonArray();
                JsonObject groupByValue = new JsonObject();
                groupByValue.addProperty("name", "value");
                groupByValue.addProperty("range_size", "10");
                groupBys.add(groupByValue);
                queryObject.add("group_by", groupBys);
                // aggregators
                JsonArray aggregators = new JsonArray();
                JsonObject sumaggregator = new JsonObject();
                sumaggregator.addProperty("name", "sum");
                sumaggregator.addProperty("align_sampling", true);
                JsonObject sampling = new JsonObject();
                sampling.addProperty("value", "1");
                sampling.addProperty("unit", "milliseconds");
                sumaggregator.add("sampling", sampling);
                aggregators.add(sumaggregator);
                queryObject.add("aggregators", aggregators);

		response.begin();
		response.formatQuery(groups, false,queryObject, 10);
		response.end();

		assertJson(writer.toString(), json);
	}

	@Test
	public void test_no_tags() throws IOException, FormatterException
	{
		String json = Resources.toString(Resources.getResource("query-response-two-groups-no-tags.json"), Charsets.UTF_8);

		ValueGroupBy groupBy = new ValueGroupBy(10);
		List<DataPointGroup> groups = new ArrayList<DataPointGroup>();

		ListDataPointGroup group1 = new ListDataPointGroup("metric1");
		group1.addTag("tag1", "value1");
		group1.addTag("tag2", "value2");
		group1.addGroupByResult(groupBy.getGroupByResult(0));
		group1.addDataPoint(new LongDataPoint(12345, 1));
		group1.addDataPoint(new LongDataPoint(56789, 2));
		group1.addDataPoint(new DoubleDataPoint(98765, 2.9));

		ListDataPointGroup group2 = new ListDataPointGroup("metric2");
		group2.addTag("tag3", "value3");
		group2.addTag("tag4", "value4");
		group2.addGroupByResult(groupBy.getGroupByResult(1));
		group2.addDataPoint(new LongDataPoint(12345, 5));
		group2.addDataPoint(new LongDataPoint(56789, 6));
		group2.addDataPoint(new DoubleDataPoint(98765, 7.9));

		groups.add(group1);
		groups.add(group2);

		response.begin();
		response.formatQuery(groups, true, 10);
		response.end();

		assertJson(writer.toString(), json);
	}

	@Test(expected = IllegalStateException.class)
	public void test_infinite_double_invalid() throws IOException, FormatterException
	{
		ValueGroupBy groupBy = new ValueGroupBy(10);
		List<DataPointGroup> groups = new ArrayList<DataPointGroup>();

		ListDataPointGroup group1 = new ListDataPointGroup("metric1");
		group1.addTag("tag1", "value1");
		group1.addTag("tag2", "value2");
		group1.addGroupByResult(groupBy.getGroupByResult(0));
		group1.addDataPoint(new LongDataPoint(12345, 1));
		group1.addDataPoint(new LongDataPoint(56789, 2));
		group1.addDataPoint(new DoubleDataPoint(98765, Double.POSITIVE_INFINITY));

		groups.add(group1);

		response.begin();
		response.formatQuery(groups, false, 10);
		response.end();
	}

	private void assertJson(String actual, String expected)
	{
		JsonObject expectedObject = (JsonObject) parser.parse(expected);
		JsonObject actualObject = (JsonObject) parser.parse(actual);

		assertThat(actualObject, equalTo(expectedObject));
	}
}