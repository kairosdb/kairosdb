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

import java.io.IOException;
import java.util.List;

public class GsonParserTest
{
	private String getJson(String resourceName) throws IOException
	{
		String json = null;
		json = Resources.toString(Resources.getResource(resourceName), Charsets.UTF_8);

		return (json);
	}

	@Test
	public void test() throws Exception
	{
		GsonParser parser = new GsonParser(new TestAggregatorFactory());
		String json = getJson("query-metric-absolute-dates.json");

		List<QueryMetric> results = parser.parseQueryMetric(json);
	}
}
