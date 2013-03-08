//
// GsonParserTest.java
//
// Copyright 2013, NextPage Inc. All rights reserved.
//

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
