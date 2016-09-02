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
package org.kairosdb.core.groupby;

import org.junit.Test;
import org.kairosdb.core.datapoints.DoubleDataPoint;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.formatter.FormatterException;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class BinGroupByTest
{

	@Test(expected = IllegalArgumentException.class)
	public void test_groupSizeZero_invalid()
	{
		double[] binValues = new double[0];
		new BinGroupBy(binValues);
	}

	@Test
	public void test_GroupByResultJson() throws FormatterException
	{
		double[] binValues = {2, 5, 7};
		BinGroupBy groupBy = new BinGroupBy(binValues);
		GroupByResult groupByResult = groupBy.getGroupByResult(3);

		assertThat(groupByResult.toJson(), equalTo("{\"name\":\"bin\",\"bins\":[2,5,7],\"group\":{\"bin_number\":3}}"));
	}

	@Test
	public void test_getGroupId_longValue()
	{
		double[] binValues = {2, 5, 7};
		Map<String, String> tags = new HashMap<String, String>();
		BinGroupBy groupBy = new BinGroupBy(binValues);

		assertThat(groupBy.getGroupId(new LongDataPoint(1, 1L), tags), equalTo(0));
		assertThat(groupBy.getGroupId(new LongDataPoint(1, 2L), tags), equalTo(1));
		assertThat(groupBy.getGroupId(new LongDataPoint(1, 4L), tags), equalTo(1));
		assertThat(groupBy.getGroupId(new LongDataPoint(1, 5L), tags), equalTo(2));
		assertThat(groupBy.getGroupId(new LongDataPoint(1, 6L), tags), equalTo(2));
		assertThat(groupBy.getGroupId(new LongDataPoint(1, 7L), tags), equalTo(3));
		assertThat(groupBy.getGroupId(new LongDataPoint(1, 8L), tags), equalTo(3));
	}

	@Test
	public void test_getGroupId_doubleValue()
	{
		double[] binValues = {2, 5, 7};
		Map<String, String> tags = new HashMap<String, String>();
		BinGroupBy groupBy = new BinGroupBy(binValues);

		assertThat(groupBy.getGroupId(new DoubleDataPoint(1, 1.9), tags), equalTo(0));
		assertThat(groupBy.getGroupId(new DoubleDataPoint(1, 2.0), tags), equalTo(1));
		assertThat(groupBy.getGroupId(new DoubleDataPoint(1, 4.9), tags), equalTo(1));
		assertThat(groupBy.getGroupId(new DoubleDataPoint(1, 5.0), tags), equalTo(2));
		assertThat(groupBy.getGroupId(new DoubleDataPoint(1, 6.9), tags), equalTo(2));
		assertThat(groupBy.getGroupId(new DoubleDataPoint(1, 7.0), tags), equalTo(3));
		assertThat(groupBy.getGroupId(new DoubleDataPoint(1, 7.1), tags), equalTo(3));
	}
}