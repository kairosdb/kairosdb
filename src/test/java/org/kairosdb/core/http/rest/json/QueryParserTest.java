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

package org.kairosdb.core.http.rest.json;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.KairosQueryProcessingChain;
import org.kairosdb.core.aggregator.TestAggregatorFactory;
import org.kairosdb.core.datastore.Duration;
import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.datastore.TimeUnit;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.core.groupby.TestGroupByFactory;
import org.kairosdb.core.http.rest.BeanValidationException;
import org.kairosdb.core.http.rest.QueryException;
import org.kairosdb.rollup.RollupTask;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.fail;

public class QueryParserTest
{
	private QueryParser parser;

	@Before
	public void setup() throws KairosDBException
	{
		parser = new QueryParser(new KairosQueryProcessingChain(new TestAggregatorFactory(), new TestGroupByFactory()), new TestQueryPluginFactory());
	}

	@Test
	public void test_absolute_dates() throws Exception
	{
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
		String json = Resources.toString(Resources.getResource("invalid-query-metric-no-aggregators.json"), Charsets.UTF_8);

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
		assertThat(QueryParser.getUnderscorePropertyName("groupBy"), equalTo("group_by"));
		assertThat(QueryParser.getUnderscorePropertyName("groupByValue"), equalTo("group_by_value"));
		assertThat(QueryParser.getUnderscorePropertyName("ABC"), equalTo("_a_b_c"));
	}

	@Test(expected = BeanValidationException.class)
	public void test_noName() throws Exception
	{
		String json = Resources.toString(Resources.getResource("invalid-query-metric-no-name.json"), Charsets.UTF_8);

		parser.parseQueryMetric(json);
	}

	@Test
	public void test_noTags() throws Exception
	{
		String json = Resources.toString(Resources.getResource("query-metric-no-tags.json"), Charsets.UTF_8);

		List<QueryMetric> results = parser.parseQueryMetric(json);

		assertThat(results.size(), equalTo(1));
		QueryMetric queryMetric = results.get(0);
		assertThat(queryMetric.getTags(), notNullValue());
	}

	@Test
	public void test_oneTag() throws Exception
	{
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

	@Test
	public void test_excludeTags() throws Exception
	{
		String json = Resources.toString(Resources.getResource("query-metric-exclude-tags.json"), Charsets.UTF_8);

		List<QueryMetric> results = parser.parseQueryMetric(json);

		assertThat(results.size(), equalTo(1));
		QueryMetric queryMetric = results.get(0);
		assertThat(queryMetric.getCacheString(), equalTo("784041330:788879730:bob:host=bar:host=foo:"));
		assertThat(queryMetric.isExcludeTags(), equalTo(true));
		assertThat(queryMetric.getTags(), notNullValue());
		assertThat(queryMetric.getTags().get("host").size(), equalTo(2));
		assertThat(queryMetric.getTags().get("host"), hasItem("bar"));
		assertThat(queryMetric.getTags().get("host"), hasItem("foo"));
	}

	@Test
	public void test_cacheTime_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("invalid-query-metric-cache-time.json"), Charsets.UTF_8);

		assertBeanValidation(json, "cache_time must be greater than or equal to 0");
	}

	@Test
	public void test_no_start_time_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("invalid-query-metric-no-start_date.json"), Charsets.UTF_8);

		assertBeanValidation(json, "query.metric[0].start_time relative or absolute time must be set");
	}

	@Test
	public void test_absoluteStartTime_before_epoch_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("query-metric-start_absolute-before-epoch.json"), Charsets.UTF_8);
		parser.parseQueryMetric(json);
	}

	@Test
	public void test_relativeStartTime_before_epoch_valid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("query-metric-relative-startTime-before-epoch.json"), Charsets.UTF_8);
		parser.parseQueryMetric(json);
	}

	@Test
	public void test_absoluteEndTime_before_startTime_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("invalid-query-metric-absoluteEndTime-less-than-startTime.json"), Charsets.UTF_8);

		assertBeanValidation(json, "query.metric[0].end_time must be greater than the start time");
	}

		@Test
	public void test_relativeEndTime_before_startTime_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("invalid-query-metric-relativeEndTime-less-than-startTime.json"), Charsets.UTF_8);

		assertBeanValidation(json, "query.metric[0].end_time must be greater than the start time");
	}

	@Test
	public void test_relativeStartTime_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("invalid-query-metric-relative-startTime-value.json"), Charsets.UTF_8);

		assertBeanValidation(json, "start_relative.value must be greater than or equal to 1");
	}

	@Test
	public void test_relativeEndTime_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("invalid-query-metric-relative-endTime-value.json"), Charsets.UTF_8);

		assertBeanValidation(json, "end_relative.value must be greater than or equal to 1");
	}

	@Test
	public void test_missingMetric_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("invalid-query-metric-no-metric.json"), Charsets.UTF_8);

		assertBeanValidation(json, "query.metric[] must have a size of at least 1");
	}

	@Test
	public void test_emptyMetricName_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("invalid-query-metric-empty-name.json"), Charsets.UTF_8);

		assertBeanValidation(json, "query.metric[0].name may not be empty");
	}

	@Test
	public void test_nullTagValueInArray_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("query-metric-null-tag-value-in-array.json"), Charsets.UTF_8);

		assertBeanValidation(json, "query.metric[0].tags[0].host value must not be null or empty");
	}

	@Test
	public void test_emptyTagName_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("invalid-query-metric-tag-empty-name.json"), Charsets.UTF_8);

		assertBeanValidation(json, "query.metric[0].tags[0] name must not be empty");
	}

	@Test
	public void test_emptyTagValueInArray_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("invalid-query-metric-tag-empty-value-in-array.json"), Charsets.UTF_8);

		assertBeanValidation(json, "query.metric[0].tags[0].host value must not be null or empty");
	}

	@Test
	public void test_nullTagValue_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("query-metric-null-tag-value.json"), Charsets.UTF_8);

		assertBeanValidation(json, "query.metric[0].tags[0].host value must not be null or empty");
	}

	@Test
	public void test_emptyTagValue_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("invalid-query-metric-tag-empty-value.json"), Charsets.UTF_8);

		assertBeanValidation(json, "query.metric[0].tags[0].host value must not be null or empty");
	}

	@Test
	public void test_aggregator_missingName_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("invalid-query-metric-aggregators-no-name.json"), Charsets.UTF_8);

		assertBeanValidation(json, "query.metric[0].aggregators[0] must have a name");
	}

	@Test
	public void test_aggregator_emptyName_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("invalid-query-metric-aggregators-empty-name.json"), Charsets.UTF_8);

		assertBeanValidation(json, "query.metric[0].aggregators[0] must have a name");
	}

	@Test
	public void test_aggregator_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("invalid-query-metric-aggregators.json"), Charsets.UTF_8);

		assertBeanValidation(json, "query.metric[0].aggregators[0].bogus invalid aggregators name");
	}

	@Test
	public void test_aggregator_sampling_value_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("invalid-query-metric-aggregators-sampling-value.json"), Charsets.UTF_8);

		assertBeanValidation(json, "query.metric[0].aggregators[0].sampling.value must be greater than or equal to 1");
	}

	@Test
	public void test_aggregator_sampling_timezone_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("invalid-query-metric-aggregators-sampling-timezone.json"), Charsets.UTF_8);

		assertBeanValidation(json, "query.bogus is not a valid time zone, must be one of " + DateTimeZone.getAvailableIDs());
	}
	
	@Test
	public void test_aggregator_sum_noSampling_valid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("invalid-query-metric-aggregators-sum-no-sampling.json"), Charsets.UTF_8);

		parser.parseQueryMetric(json);
	}

	@Test
	public void test_aggregator_div_no_divisor_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("invalid-query-metric-aggregators-div-no-divisor.json"), Charsets.UTF_8);

		assertBeanValidation(json, "query.metric[0].aggregators[0].m_divisor may not be zero");
	}

	@Test
	public void test_aggregator_div_divisor_zero_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("invalid-query-metric-aggregators-div-divisor-zero.json"), Charsets.UTF_8);

		assertBeanValidation(json, "query.metric[0].aggregators[0].m_divisor may not be zero");
	}

	@Test
	public void test_aggregator_percentile_no_percentile_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("invalid-query-metric-aggregators-percentile-no-percentile.json"), Charsets.UTF_8);

		assertBeanValidation(json, "query.metric[0].aggregators[0].percentile may not be zero");
	}

	@Test
	public void test_aggregator_percentile_percentile_zero_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("invalid-query-metric-aggregators-percentile-percentile-zero.json"), Charsets.UTF_8);

		assertBeanValidation(json, "query.metric[0].aggregators[0].percentile may not be zero");
	}

	@Test
	public void test_aggregator_percentile_percentile_numberFormat_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("invalid-query-metric-aggregators-percentile-percentile.json"), Charsets.UTF_8);

		assertBeanValidation(json, "query.metric[0].aggregators[0].percentile multiple points");
	}

	@Test
	public void test_aggregator_sampling_unit_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("invalid-query-metric-aggregators-sampling-unit.json"), Charsets.UTF_8);

		assertBeanValidation(json,
				"query.metric[0].aggregators[0].bogus is not a valid time unit, must be one of MILLISECONDS,SECONDS,MINUTES,HOURS,DAYS,WEEKS,MONTHS,YEARS");
	}

	@Test
	public void test_groupby_missingName_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("invalid-query-metric-group_by-no-name.json"), Charsets.UTF_8);

		assertBeanValidation(json, "query.metric[0].group_by[0] must have a name");
	}

	@Test
	public void test_groupby_emptyName_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("invalid-query-metric-group_by-empty-name.json"), Charsets.UTF_8);

		assertBeanValidation(json, "query.metric[0].group_by[1] must have a name");
	}

	@Test
	public void test_groupby_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("invalid-query-metric-group_by.json"), Charsets.UTF_8);

		assertBeanValidation(json, "query.metric[0].group_by[0].bogus invalid group_by name");
	}

	@Test
	public void test_groupby_tag_missing_tags_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("invalid-query-metric-group_by-tag-missing-tags.json"), Charsets.UTF_8);

		assertBeanValidation(json, "query.metric[0].group_by[0].tags may not be null");
	}

	@Test
	public void test_groupby_tag_emtpy_tags_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("invalid-query-metric-group_by-tag-empty-tags.json"), Charsets.UTF_8);

		assertBeanValidation(json, "query.metric[0].group_by[0].tags may not be empty");
	}

	@Test
	public void test_groupby_time_missing_range_size_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("invalid-query-metric-group_by-time-missing-range_size.json"), Charsets.UTF_8);

		assertBeanValidation(json, "query.metric[0].group_by[0].rangeSize may not be null");
	}

	@Test
	public void test_groupby_time_range_size_value_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("invalid-query-metric-group_by-time-range_size_value.json"), Charsets.UTF_8);

		assertBeanValidation(json, "query.metric[0].group_by[0].range_size.value must be greater than or equal to 1");
	}

	@Test
	public void test_groupby_time_range_size_unit_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("invalid-query-metric-group_by-time-range_size_unit.json"), Charsets.UTF_8);

		assertBeanValidation(json, "query.metric[0].group_by[0].bogus is not a valid time unit, must be one of MILLISECONDS,SECONDS,MINUTES,HOURS,DAYS,WEEKS,MONTHS,YEARS");
	}

	@Test
	public void test_groupby_time_missing_group_count_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("invalid-query-metric-group_by-time-missing-group_count.json"), Charsets.UTF_8);

		assertBeanValidation(json, "query.metric[0].group_by[0].groupCount must be greater than or equal to 1");
	}

	@Test
	public void test_groupby_value_range_size_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("invalid-query-metric-group_by-value-range_size.json"), Charsets.UTF_8);

		assertBeanValidation(json, "query.metric[0].group_by[0].range_size.value must be greater than or equal to 1");
	}

	@Test
	public void test_parseRollUpTask_empty_name_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("invalid-rollup-no-name-empty.json"), Charsets.UTF_8);

		assertRollupBeanValidation(json, "name may not be empty");
	}

	@Test
	public void test_parseRollUpTask_no_execution_interval_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("invalid-rollup-no-execution_interval.json"), Charsets.UTF_8);

		assertRollupBeanValidation(json, "executionInterval may not be null");
	}

	@Test
	public void test_parseRollUpTask_empty_saveAs_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("invalid-rollup-no-saveAs.json"), Charsets.UTF_8);

		assertRollupBeanValidation(json, "rollup[0].saveAs may not be empty");
	}

	/**
	 Test the parsing of the query. Only a sanity check since it parseRollupTask
	 reuses parseQuery.
	 */
	@Test
	public void test_parseRollUpTask_empty_query_time_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("invalid-rollup-no-query_time.json"), Charsets.UTF_8);

		assertRollupBeanValidation(json, "rollup[0].query.metric[0].start_time relative or absolute time must be set");
	}

	@Test
	public void test_parseRollUpTask_noRangeAggregator_invalid() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("invalid-rollup-no-range-aggregator.json"), Charsets.UTF_8);

		assertRollupBeanValidation(json, "rollup[0].query[0].aggregator At least one aggregator must be a range aggregator");
	}

	@Test
	public void test_parseRollupTask() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("rolluptask1.json"), Charsets.UTF_8);

		RollupTask task = parser.parseRollupTask(json);

		assertThat(task.getName(), equalTo("Rollup1"));
		assertThat(task.getExecutionInterval(), equalTo(new Duration(1, TimeUnit.HOURS)));
		assertThat(task.getRollups().size(), equalTo(1));
		assertThat(task.getRollups().get(0).getSaveAs(), equalTo("kairosdb.http.query_time_rollup"));
		assertThat(task.getRollups().get(0).getQueryMetrics().size(), equalTo(1));
		assertThat(task.getRollups().get(0).getQueryMetrics().get(0).getName(), equalTo("kairosdb.http.query_time"));
	}

	@Test
	public void test_parseRollupTasks() throws IOException, QueryException
	{
		String json = Resources.toString(Resources.getResource("rolluptasks.json"), Charsets.UTF_8);

		List<RollupTask> tasks = parser.parseRollupTasks(json);

		assertThat(tasks.size(), equalTo(2));

		assertThat(tasks.get(0).getName(), equalTo("Rollup1"));
		assertThat(tasks.get(0).getExecutionInterval(), equalTo(new Duration(1, TimeUnit.HOURS)));
		assertThat(tasks.get(0).getRollups().size(), equalTo(1));
		assertThat(tasks.get(0).getRollups().get(0).getSaveAs(), equalTo("kairosdb.http.query_time_rollup"));
		assertThat(tasks.get(0).getRollups().get(0).getQueryMetrics().size(), equalTo(1));
		assertThat(tasks.get(0).getRollups().get(0).getQueryMetrics().get(0).getName(), equalTo("kairosdb.http.query_time"));

		assertThat(tasks.get(1).getName(), equalTo("Rollup2"));
		assertThat(tasks.get(1).getExecutionInterval(), equalTo(new Duration(1, TimeUnit.MINUTES)));
		assertThat(tasks.get(1).getRollups().size(), equalTo(1));
		assertThat(tasks.get(1).getRollups().get(0).getSaveAs(), equalTo("kairosdb.http.foo_rollup"));
		assertThat(tasks.get(1).getRollups().get(0).getQueryMetrics().size(), equalTo(1));
		assertThat(tasks.get(1).getRollups().get(0).getQueryMetrics().get(0).getName(), equalTo("kairosdb.http.foo"));
	}

	private void assertRollupBeanValidation(String json, String expectedMessage)
	{
		try
		{
			parser.parseRollupTask(json);
			fail("Expected BeanValidationException");
		}
		catch (QueryException e)
		{
			fail("Expected BeanValidationException");
		}
		catch (BeanValidationException e)
		{
			assertThat(e.getErrorMessages().size(), equalTo(1));
			assertThat(e.getErrorMessages().get(0), equalTo(expectedMessage));
		}
	}

	private void assertBeanValidation(String json, String expectedMessage)
	{
		try
		{
			parser.parseQueryMetric(json);
			fail("Expected BeanValidationException");
		}
		catch (QueryException e)
		{
			fail("Expected BeanValidationException");
		}
		catch (BeanValidationException e)
		{
			assertThat(e.getErrorMessages().size(), equalTo(1));
			assertThat(e.getErrorMessages().get(0), equalTo(expectedMessage));
		}
	}
}
