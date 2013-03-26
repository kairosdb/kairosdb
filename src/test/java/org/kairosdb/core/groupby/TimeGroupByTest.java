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
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datastore.Duration;
import org.kairosdb.core.datastore.TimeUnit;
import org.kairosdb.core.formatter.FormatterException;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class TimeGroupByTest
{
	@Test
	public void test_getGroupByResults() throws FormatterException
	{
		TimeGroupBy groupBy = new TimeGroupBy(new Duration(2, TimeUnit.DAYS), 14);

		GroupByResult groupByResult = groupBy.getGroupByResult(2);

		assertThat(groupByResult.toJson(), equalTo("{\"name\":\"time\",\"target_size\":{\"value\":2,\"unit\":\"DAYS\"},\"group_count\":14,\"group\":{\"group_number\":2}}"));
	}

	@Test
	public void test_getGroupId_long()
	{
		Map<String, String> tags = new HashMap<String, String>();
		TimeGroupBy groupBy = new TimeGroupBy(new Duration(1, TimeUnit.DAYS), 7);

		// Set start time to be Sunday a week ago
		long sunday = dayOfWeek(Calendar.SUNDAY);
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(sunday);
		cal.add(Calendar.WEEK_OF_MONTH, -1);

		groupBy.setStartDate(cal.getTime().getTime());

		assertThat(groupBy.getGroupId(new DataPoint(dayOfWeek(Calendar.SUNDAY), 1), tags), equalTo(0));
		assertThat(groupBy.getGroupId(new DataPoint(dayOfWeek(Calendar.MONDAY), 1), tags), equalTo(1));
		assertThat(groupBy.getGroupId(new DataPoint(dayOfWeek(Calendar.TUESDAY), 1), tags), equalTo(2));
		assertThat(groupBy.getGroupId(new DataPoint(dayOfWeek(Calendar.WEDNESDAY), 1), tags), equalTo(3));
		assertThat(groupBy.getGroupId(new DataPoint(dayOfWeek(Calendar.THURSDAY), 1), tags), equalTo(4));
		assertThat(groupBy.getGroupId(new DataPoint(dayOfWeek(Calendar.FRIDAY), 1), tags), equalTo(5));
		assertThat(groupBy.getGroupId(new DataPoint(dayOfWeek(Calendar.SATURDAY), 1), tags), equalTo(6));
	}

	private long dayOfWeek(int dayOfWeek)
	{
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.DAY_OF_WEEK, dayOfWeek);
		return cal.getTime().getTime();
	}
}