//
//  TagGroupByResultTest.java
//
// Copyright 2016, KairosDB Authors
//        
package org.kairosdb.core.groupby;

import org.junit.Test;
import org.kairosdb.core.formatter.FormatterException;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class TagGroupByResultTest
{

	@Test(expected = NullPointerException.class)
	public void test_nullGroupBy_invalid()
	{
		 new TagGroupByResult(null, new HashMap<String, String>());
	}

	@Test(expected = NullPointerException.class)
	public void test_nullTagResults_invalid()
	{
		 new TagGroupByResult(new TagGroupBy("foo"), null);
	}

	@Test
	public void test_toJson() throws FormatterException
	{
		TagGroupBy groupBy = new TagGroupBy("tag1", "tag2");
		Map<String, String> tagResults = new LinkedHashMap<String, String>();
		tagResults.put("tag1", "result1");
		tagResults.put("tag2", "result2");

		TagGroupByResult result = new TagGroupByResult(groupBy, tagResults);

		assertThat(result.toJson(), equalTo("{\"name\":\"tag\",\"tags\":[\"tag1\",\"tag2\"],\"group\":{\"tag1\":\"result1\",\"tag2\":\"result2\"}}"));
	}
}