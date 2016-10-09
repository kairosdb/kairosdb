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
package org.kairosdb.core.groupby;

import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datastore.Duration;
import org.kairosdb.core.datastore.TimeUnit;
import org.kairosdb.core.formatter.FormatterException;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class TimeGroupByTest
{
	@Test
	public void test_getGroupByResults() throws FormatterException
	{
		TimeGroupBy groupBy = new TimeGroupBy(new Duration(2, TimeUnit.DAYS), 14);

		GroupByResult groupByResult = groupBy.getGroupByResult(2);

		assertThat(groupByResult.toJson(), equalTo("{\"name\":\"time\",\"range_size\":{\"value\":2,\"unit\":\"DAYS\"},\"group_count\":14,\"group\":{\"group_number\":2}}"));
	}

	@Test
	public void test_getGroupId()
	{
		Map<String, String> tags = new HashMap<String, String>();
		TimeGroupBy groupBy = new TimeGroupBy(new Duration(1, TimeUnit.DAYS), 7);

		// Set start time to be Sunday a week ago
		long sunday = dayOfWeek(Calendar.SUNDAY);
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		cal.setTimeInMillis(sunday);
		cal.add(Calendar.WEEK_OF_MONTH, -1);

		groupBy.setStartDate(cal.getTimeInMillis());

		assertThat(groupBy.getGroupId(new LongDataPoint(dayOfWeek(Calendar.SUNDAY), 1), tags), equalTo(0));
		assertThat(groupBy.getGroupId(new LongDataPoint(dayOfWeek(Calendar.MONDAY), 1), tags), equalTo(1));
		assertThat(groupBy.getGroupId(new LongDataPoint(dayOfWeek(Calendar.TUESDAY), 1), tags), equalTo(2));
		assertThat(groupBy.getGroupId(new LongDataPoint(dayOfWeek(Calendar.WEDNESDAY), 1), tags), equalTo(3));
		assertThat(groupBy.getGroupId(new LongDataPoint(dayOfWeek(Calendar.THURSDAY), 1), tags), equalTo(4));
		assertThat(groupBy.getGroupId(new LongDataPoint(dayOfWeek(Calendar.FRIDAY), 1), tags), equalTo(5));
		assertThat(groupBy.getGroupId(new LongDataPoint(dayOfWeek(Calendar.SATURDAY), 1), tags), equalTo(6));
	}

	@Test
	public void test_getGroupId_Month()
	{
		Map<String, String> tags = new HashMap<String, String>();
		TimeGroupBy groupBy = new TimeGroupBy(new Duration(1, TimeUnit.MONTHS), 24);

		// Set start time to Jan 1, 2010 - 1 am
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		cal.set(2010, Calendar.JANUARY, 1, 1, 0, 0);
		cal.set(Calendar.MILLISECOND, 0);

		groupBy.setStartDate(cal.getTimeInMillis());

		// 2010
		assertThat(groupBy.getGroupId(new LongDataPoint(dayOfMonth(2010, Calendar.JANUARY, 1), 1), tags), equalTo(0));
		assertThat(groupBy.getGroupId(new LongDataPoint(dayOfMonth(2010, Calendar.JANUARY, 31), 1), tags), equalTo(0));
		assertThat(groupBy.getGroupId(new LongDataPoint(dayOfMonth(2010, Calendar.FEBRUARY, 1), 1), tags), equalTo(1));
		assertThat(groupBy.getGroupId(new LongDataPoint(dayOfMonth(2010, Calendar.FEBRUARY, 28), 1), tags), equalTo(1));
		assertThat(groupBy.getGroupId(new LongDataPoint(dayOfMonth(2010, Calendar.MARCH, 1), 1), tags), equalTo(2));
		assertThat(groupBy.getGroupId(new LongDataPoint(dayOfMonth(2010, Calendar.MARCH, 31), 1), tags), equalTo(2));
		assertThat(groupBy.getGroupId(new LongDataPoint(dayOfMonth(2010, Calendar.JULY, 1), 1), tags), equalTo(6));
		assertThat(groupBy.getGroupId(new LongDataPoint(dayOfMonth(2010, Calendar.JULY, 31), 1), tags), equalTo(6));
		assertThat(groupBy.getGroupId(new LongDataPoint(dayOfMonth(2010, Calendar.DECEMBER, 1), 1), tags), equalTo(11));
		assertThat(groupBy.getGroupId(new LongDataPoint(dayOfMonth(2010, Calendar.DECEMBER, 31), 1), tags), equalTo(11));

		// 2011
		assertThat(groupBy.getGroupId(new LongDataPoint(dayOfMonth(2011, Calendar.JANUARY, 31), 1), tags), equalTo(12));
		assertThat(groupBy.getGroupId(new LongDataPoint(dayOfMonth(2011, Calendar.FEBRUARY, 28), 1), tags), equalTo(13));

		// 2012
		assertThat(groupBy.getGroupId(new LongDataPoint(dayOfMonth(2012, Calendar.JANUARY, 31), 1), tags), equalTo(0));
		assertThat(groupBy.getGroupId(new LongDataPoint(dayOfMonth(2012, Calendar.FEBRUARY, 28), 1), tags), equalTo(1));
	}

	private long dayOfWeek(int dayOfWeek)
	{
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.DAY_OF_WEEK, dayOfWeek);
		return cal.getTime().getTime();
	}

	private long dayOfMonth(int year, int month, int dayOfMonth)
	{
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		cal.set(year, month, dayOfMonth, 0, 0, 0);
		cal.set(Calendar.MILLISECOND, 0);

		return cal.getTimeInMillis();
	}
}