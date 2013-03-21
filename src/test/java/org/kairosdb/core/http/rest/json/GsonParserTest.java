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
import org.junit.Test;
import org.kairosdb.core.aggregator.TestAggregatorFactory;
import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.groupby.TestGroupByFactory;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

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
	public void test_underscoreConverter()
	{
		assertThat(GsonParser.getUnderscorePropertyName("groupBy"), equalTo("group_by"));
		assertThat(GsonParser.getUnderscorePropertyName("groupByValue"), equalTo("group_by_value"));
		assertThat(GsonParser.getUnderscorePropertyName("ABC"), equalTo("_a_b_c"));
	}

}
