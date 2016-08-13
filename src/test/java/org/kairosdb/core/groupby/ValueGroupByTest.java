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

public class ValueGroupByTest
{

	@Test(expected = IllegalArgumentException.class)
	public void test_groupSizeZero_invalid()
	{
		new ValueGroupBy(0);
	}

	@Test(expected = IllegalArgumentException.class)
	public void test_groupSizeNegative_invalid()
	{
		new ValueGroupBy(-1);
	}

	@Test
	public void test_GroupByResultJson() throws FormatterException
	{
		ValueGroupBy groupBy = new ValueGroupBy(3);

		GroupByResult groupByResult = groupBy.getGroupByResult(2);

		assertThat(groupByResult.toJson(), equalTo("{\"name\":\"value\",\"range_size\":3,\"group\":{\"group_number\":2}}"));
	}

	@Test
	public void test_getGroupId_longValue()
	{
		Map<String, String> tags = new HashMap<String, String>();
		ValueGroupBy groupBy = new ValueGroupBy(3);

		assertThat(groupBy.getGroupId(new LongDataPoint(1, 0L), tags), equalTo(0));
		assertThat(groupBy.getGroupId(new LongDataPoint(1, 1L), tags), equalTo(0));
		assertThat(groupBy.getGroupId(new LongDataPoint(1, 2L), tags), equalTo(0));
		assertThat(groupBy.getGroupId(new LongDataPoint(1, 3L), tags), equalTo(1));
		assertThat(groupBy.getGroupId(new LongDataPoint(1, 4L), tags), equalTo(1));
		assertThat(groupBy.getGroupId(new LongDataPoint(1, 5L), tags), equalTo(1));
		assertThat(groupBy.getGroupId(new LongDataPoint(1, 6L), tags), equalTo(2));
	}

	@Test
	public void test_getGroupId_doubleValue()
	{
		Map<String, String> tags = new HashMap<String, String>();
		ValueGroupBy groupBy = new ValueGroupBy(3);

		assertThat(groupBy.getGroupId(new DoubleDataPoint(1, 0.0), tags), equalTo(0));
		assertThat(groupBy.getGroupId(new DoubleDataPoint(1, 1.2), tags), equalTo(0));
		assertThat(groupBy.getGroupId(new DoubleDataPoint(1, 2.3), tags), equalTo(0));
		assertThat(groupBy.getGroupId(new DoubleDataPoint(1, 3.3), tags), equalTo(1));
		assertThat(groupBy.getGroupId(new DoubleDataPoint(1, 4.4), tags), equalTo(1));
		assertThat(groupBy.getGroupId(new DoubleDataPoint(1, 5.2), tags), equalTo(1));
		assertThat(groupBy.getGroupId(new DoubleDataPoint(1, 6.1), tags), equalTo(2));
	}
}