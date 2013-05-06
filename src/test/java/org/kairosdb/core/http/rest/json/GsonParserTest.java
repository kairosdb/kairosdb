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

package org.kairosdb.core.http.rest.json;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.google.gson.JsonSyntaxException;
import org.junit.Test;
import org.kairosdb.core.aggregator.TestAggregatorFactory;
import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.groupby.TestGroupByFactory;
import org.kairosdb.core.http.rest.BeanValidationException;
import org.kairosdb.core.http.rest.QueryException;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class GsonParserTest
{
	@Test
	public void test() throws Exception
	{
		GsonParser parser = new GsonParser(new TestAggregatorFactory(), new TestGroupByFactory());
		String json = Resources.toString(Resources.getResource("query-metric-absolute-dates-with-groupby.json"), Charsets.UTF_8);

		List<QueryMetric> results = parser.parseQueryMetric(json);

		assertThat(results.size(), equalTo(1));

		QueryMetric queryMetric = results.get(0);
		assertThat(queryMetric.getName(), equalTo("abc.123"));
		assertThat(queryMetric.getStartTime(), equalTo(784041330L));
		assertThat(queryMetric.getEndTime(), equalTo(788879730L));
		assertThat(queryMetric.getAggregators().size(), equalTo(1));
		assertThat(queryMetric.getGroupBys().size(), equalTo(2));
	}

	@Test
	public void test_withNoAggregators() throws Exception
	{
		GsonParser parser = new GsonParser(new TestAggregatorFactory(), new TestGroupByFactory());
		String json = Resources.toString(Resources.getResource("query-metric-no-aggregators.json"), Charsets.UTF_8);

		List<QueryMetric> results = parser.parseQueryMetric(json);

		assertThat(results.size(), equalTo(1));

		QueryMetric queryMetric = results.get(0);
		assertThat(queryMetric.getName(), equalTo("abc.123"));
		assertThat(queryMetric.getStartTime(), equalTo(784041330L));
		assertThat(queryMetric.getEndTime(), equalTo(788879730L));
		assertThat(queryMetric.getAggregators().size(), equalTo(0));
		assertThat(queryMetric.getGroupBys().size(), equalTo(2));
	}

	@Test
	public void test_underscoreConverter()
	{
		assertThat(GsonParser.getUnderscorePropertyName("groupBy"), equalTo("group_by"));
		assertThat(GsonParser.getUnderscorePropertyName("groupByValue"), equalTo("group_by_value"));
		assertThat(GsonParser.getUnderscorePropertyName("ABC"), equalTo("_a_b_c"));
	}

	@Test(expected=BeanValidationException.class)
	public void test_noName() throws Exception
	{
		GsonParser parser = new GsonParser(new TestAggregatorFactory(), new TestGroupByFactory());
		String json = Resources.toString(Resources.getResource("query-metric-no-name.json"), Charsets.UTF_8);

		List<QueryMetric> results = parser.parseQueryMetric(json);
	}

	@Test
	public void test_noTags() throws Exception
	{
		GsonParser parser = new GsonParser(new TestAggregatorFactory(), new TestGroupByFactory());
		String json = Resources.toString(Resources.getResource("query-metric-no-tags.json"), Charsets.UTF_8);

		List<QueryMetric> results = parser.parseQueryMetric(json);

		assertThat(results.size(), equalTo(1));
		QueryMetric queryMetric = results.get(0);
		assertThat(queryMetric.getTags(), notNullValue());
	}

	@Test
	public void test_oneTag() throws Exception
	{
		GsonParser parser = new GsonParser(new TestAggregatorFactory(), new TestGroupByFactory());
		String json = Resources.toString(Resources.getResource("query-metric-one-tag.json"), Charsets.UTF_8);

		List<QueryMetric> results = parser.parseQueryMetric(json);

		assertThat(results.size(), equalTo(1));
		QueryMetric queryMetric = results.get(0);
		assertThat(queryMetric.getTags(), notNullValue());
		assertThat(queryMetric.getTags().get("host").size(), equalTo(1));
		assertThat(queryMetric.getTags().get("host"), hasItem("bar"));
	}

	@Test
	public void test_twoTags() throws Exception
	{
		GsonParser parser = new GsonParser(new TestAggregatorFactory(), new TestGroupByFactory());
		String json = Resources.toString(Resources.getResource("query-metric-two-tags.json"), Charsets.UTF_8);

		List<QueryMetric> results = parser.parseQueryMetric(json);

		assertThat(results.size(), equalTo(1));
		QueryMetric queryMetric = results.get(0);
		assertThat(queryMetric.getCacheString(), equalTo("784041330:788879730:bob:host=bar:host=foo:"));
		assertThat(queryMetric.getTags(), notNullValue());
		assertThat(queryMetric.getTags().get("host").size(), equalTo(2));
		assertThat(queryMetric.getTags().get("host"), hasItem("bar"));
		assertThat(queryMetric.getTags().get("host"), hasItem("foo"));
	}

	@Test(expected = JsonSyntaxException.class)
	public void test_nullTagValueInArray() throws IOException, QueryException
	{
		GsonParser parser = new GsonParser(new TestAggregatorFactory(), new TestGroupByFactory());
		String json = Resources.toString(Resources.getResource("query-metric-null-tag-value-in-array.json"), Charsets.UTF_8);

		parser.parseQueryMetric(json);
	}

	@Test(expected = JsonSyntaxException.class)
	public void test_emptyTagValueInArray() throws IOException, QueryException
	{
		GsonParser parser = new GsonParser(new TestAggregatorFactory(), new TestGroupByFactory());
		String json = Resources.toString(Resources.getResource("query-metric-empty-tag-value-in-array.json"), Charsets.UTF_8);

		parser.parseQueryMetric(json);
	}

	@Test(expected = JsonSyntaxException.class)
	public void test_nullTagValue() throws IOException, QueryException
	{
		GsonParser parser = new GsonParser(new TestAggregatorFactory(), new TestGroupByFactory());
		String json = Resources.toString(Resources.getResource("query-metric-null-tag-value.json"), Charsets.UTF_8);

		parser.parseQueryMetric(json);
	}

	@Test(expected = JsonSyntaxException.class)
	public void test_emptyTagValue() throws IOException, QueryException
	{
		GsonParser parser = new GsonParser(new TestAggregatorFactory(), new TestGroupByFactory());
		String json = Resources.toString(Resources.getResource("query-metric-empty-tag-value.json"), Charsets.UTF_8);

		parser.parseQueryMetric(json);
	}

	@Test(expected = JsonSyntaxException.class)
	public void test_emptyTagName() throws IOException, QueryException
	{
		GsonParser parser = new GsonParser(new TestAggregatorFactory(), new TestGroupByFactory());
		String json = Resources.toString(Resources.getResource("query-metric-empty-tag-name.json"), Charsets.UTF_8);

		parser.parseQueryMetric(json);
	}
}
