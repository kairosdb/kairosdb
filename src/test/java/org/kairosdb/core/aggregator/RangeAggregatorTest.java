//
// RangeAggregatorTest.java
//
// Copyright 2013, NextPage Inc. All rights reserved.
//

package org.kairosdb.core.aggregator;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datapoints.DoubleDataPointFactoryImpl;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.TimeUnit;
import org.kairosdb.testing.ListDataPointGroup;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class RangeAggregatorTest
{

	@Test
	public void test_yearAggregationWithoutLeapYears()
	{
		DateTimeZone utc = DateTimeZone.UTC;
		ListDataPointGroup dpGroup = new ListDataPointGroup("range_test");

		DateTime startDate = new DateTime(2014, 1, 1, 0, 0, utc);
		for (DateTime date = startDate;
		     date.isBefore(new DateTime(2015, 6, 2, 0, 0, utc));
		     date = date.plusMonths(1))
		{
			dpGroup.addDataPoint(new LongDataPoint(date.getMillis(), 1));
		}

		SumAggregator agg = new SumAggregator(new DoubleDataPointFactoryImpl());
		agg.setSampling(new Sampling(1, TimeUnit.YEARS));
		agg.setAlignSampling(false);
		agg.setStartTime(startDate.getMillis());

		DataPointGroup aggregated = agg.aggregate(dpGroup);

		assertThat(aggregated.hasNext(), is(true));
		assertThat(aggregated.next().getLongValue(), is(12L));

		assertThat(aggregated.hasNext(), is(true));
		assertThat(aggregated.next().getLongValue(), is(6L));

		assertThat(aggregated.hasNext(), is(false));
	}

	@Test
	public void test_multipleMonthAggregationWithUTC()
	{
		ListDataPointGroup dpGroup = new ListDataPointGroup("range_test");
		DateTimeZone utc = DateTimeZone.UTC;

		DateTime startDate = new DateTime(2014, 1, 1, 1, 1, utc); // LEAP year
		DateTime stopDate = new DateTime(2014, 7, 10, 1, 1, utc);
		for (DateTime iterationDT = startDate;
		     iterationDT.isBefore(stopDate);
		     iterationDT = iterationDT.plusDays(1))
		{
			dpGroup.addDataPoint(new LongDataPoint(iterationDT.getMillis(), 1));
		}

		SumAggregator agg = new SumAggregator(new DoubleDataPointFactoryImpl());
		agg.setSampling(new Sampling(3, TimeUnit.MONTHS));
		agg.setAlignSampling(false);
		agg.setStartTime(startDate.getMillis());

		DataPointGroup dpg = agg.aggregate(dpGroup);

		assertThat(dpg.hasNext(), is(true));
		DataPoint next = dpg.next();
		assertThat(new DateTime(next.getTimestamp(), utc), is(new DateTime(2014, 1, 1, 1, 1, utc)));
		assertThat(next.getLongValue(), is(90L)); // 31 + 28 + 31

		assertThat(dpg.hasNext(), is(true));
		next = dpg.next();
		assertThat(new DateTime(next.getTimestamp(), utc), is(new DateTime(2014, 4, 1, 1, 1, utc)));
		assertThat(next.getLongValue(), is(91L)); // 30 + 31 + 30

		assertThat(dpg.hasNext(), is(true));
		next = dpg.next();
		assertThat(new DateTime(next.getTimestamp(), utc), is(new DateTime(2014, 7, 1, 1, 1, utc)));
		assertThat(next.getLongValue(), is(9L)); // 10

		assertThat(dpg.hasNext(), is(false));
	}

	@Test
	public void test_multipleMonthAggregationWithUTCCheckAlignmentStartTime()
	{
		ListDataPointGroup dpGroup = new ListDataPointGroup("range_test");
		DateTimeZone utc = DateTimeZone.UTC;

		DateTime startDate = new DateTime(2014, 1, 1, 1, 1, utc); // LEAP year
		DateTime stopDate = new DateTime(2014, 7, 10, 1, 1, utc);
		for (DateTime iterationDT = startDate;
		     iterationDT.isBefore(stopDate);
		     iterationDT = iterationDT.plusDays(5))
		{
			dpGroup.addDataPoint(new LongDataPoint(iterationDT.getMillis(), 1));
		}

		SumAggregator agg = new SumAggregator(new DoubleDataPointFactoryImpl());
		agg.setTimeZone(utc);
		agg.setSampling(new Sampling(3, TimeUnit.MONTHS));
		agg.setAlignSampling(true);
		agg.setAlignStartTime(true);
		agg.setStartTime(startDate.getMillis());

		DataPointGroup dpg = agg.aggregate(dpGroup);

		assertThat(dpg.hasNext(), is(true));
		DataPoint next = dpg.next();
		assertThat(new DateTime(next.getTimestamp(), utc), is(new DateTime(2014, 1, 1, 0, 0, utc)));

		assertThat(dpg.hasNext(), is(true));
		next = dpg.next();
		assertThat(new DateTime(next.getTimestamp(), utc), is(new DateTime(2014, 4, 1, 0, 0, utc)));

		assertThat(dpg.hasNext(), is(true));
		next = dpg.next();
		assertThat(new DateTime(next.getTimestamp(), utc), is(new DateTime(2014, 7, 1, 0, 0, utc)));

		assertThat(dpg.hasNext(), is(false));
	}

	@Test
	public void test_multipleMonthAggregationWithUTCCheckAlignmentEndTime()
	{
		ListDataPointGroup dpGroup = new ListDataPointGroup("range_test");
		DateTimeZone utc = DateTimeZone.UTC;

		DateTime startDate = new DateTime(2014, 1, 1, 1, 1, utc); // LEAP year
		DateTime stopDate = new DateTime(2014, 7, 10, 1, 1, utc);
		for (DateTime iterationDT = startDate;
		     iterationDT.isBefore(stopDate);
		     iterationDT = iterationDT.plusDays(5))
		{
			dpGroup.addDataPoint(new LongDataPoint(iterationDT.getMillis(), 1));
		}

		SumAggregator agg = new SumAggregator(new DoubleDataPointFactoryImpl());
		agg.setTimeZone(utc);
		agg.setSampling(new Sampling(3, TimeUnit.MONTHS));
		agg.setAlignSampling(true);
		agg.setAlignEndTime(true);
		agg.setStartTime(startDate.getMillis());

		DataPointGroup dpg = agg.aggregate(dpGroup);

		assertThat(dpg.hasNext(), is(true));
		DataPoint next = dpg.next();
		assertThat(new DateTime(next.getTimestamp(), utc), is(new DateTime(2014, 4, 1, 0, 0, utc)));

		assertThat(dpg.hasNext(), is(true));
		next = dpg.next();
		assertThat(new DateTime(next.getTimestamp(), utc), is(new DateTime(2014, 7, 1, 0, 0, utc)));

		assertThat(dpg.hasNext(), is(true));
		next = dpg.next();
		assertThat(new DateTime(next.getTimestamp(), utc), is(new DateTime(2014, 10, 1, 0, 0, utc)));

		assertThat(dpg.hasNext(), is(false));
	}

	@Test
	public void test_aggregateByMonthStartAtMidMonthDontAlign()
	{
		ListDataPointGroup dpGroup = new ListDataPointGroup("range_test");
		DateTimeZone utc = DateTimeZone.UTC;

		DateTime startDate = new DateTime(2014, 3, 10, 0, 0, utc);
		DateTime stopDate = new DateTime(2014, 5, 23, 0, 0, utc);
		for (DateTime iterationDT = startDate;
		     iterationDT.isBefore(stopDate);
		     iterationDT = iterationDT.plusDays(1))
		{
			dpGroup.addDataPoint(new LongDataPoint(iterationDT.getMillis(), 1));
		}

		SumAggregator aggregator = new SumAggregator(new DoubleDataPointFactoryImpl());
		aggregator.setSampling(new Sampling(1, TimeUnit.MONTHS));
		aggregator.setAlignSampling(false);
		aggregator.setStartTime(startDate.getMillis());

		DataPointGroup aggregated = aggregator.aggregate(dpGroup);

		assertThat(aggregated.hasNext(), is(true));
		DataPoint marchDataPoint = aggregated.next();
		assertThat(new DateTime(marchDataPoint.getTimestamp(), utc), equalTo(startDate));
		assertThat(marchDataPoint.getLongValue(), is(31L));

		assertThat(aggregated.hasNext(), is(true));
		DataPoint aprilDataPoint = aggregated.next();
		assertThat(new DateTime(aprilDataPoint.getTimestamp(), utc), equalTo(startDate.plusMonths(1)));
		assertThat(aprilDataPoint.getLongValue(), is(30L));

		assertThat(aggregated.hasNext(), is(true));
		DataPoint mayMonthDataPoint = aggregated.next();
		assertThat(new DateTime(mayMonthDataPoint.getTimestamp(), utc), equalTo(startDate.plusMonths(2)));
		assertThat(mayMonthDataPoint.getLongValue(), is(13L));

		assertThat(aggregated.hasNext(), is(false));
	}

	@Test
	public void test_aggregateByMonthStartAtMidMonthAlign()
	{
		DateTimeZone utc = DateTimeZone.UTC;

		DateTime startDate = new DateTime(2014, 3, 10, 0, 0, utc);
		DateTime stopDate = new DateTime(2014, 5, 23, 0, 0, utc);
		ListDataPointGroup dpGroup = new ListDataPointGroup("range_test");
		for (DateTime iterationDT = startDate;
		     iterationDT.isBefore(stopDate);
		     iterationDT = iterationDT.plusDays(1))
		{
			dpGroup.addDataPoint(new LongDataPoint(iterationDT.getMillis(), 1));
		}

		SumAggregator aggregator = new SumAggregator(new DoubleDataPointFactoryImpl());
		aggregator.setSampling(new Sampling(1, TimeUnit.MONTHS));
		aggregator.setAlignSampling(true);
		aggregator.setAlignStartTime(true);
		aggregator.setStartTime(startDate.getMillis());

		DataPointGroup aggregated = aggregator.aggregate(dpGroup);

		assertThat(aggregated.hasNext(), is(true));
		DateTime marchFirst = new DateTime(2014, 3, 1, 0, 0, utc);
		DataPoint marchDataPoint = aggregated.next();
		assertThat(new DateTime(marchDataPoint.getTimestamp(), utc), equalTo(marchFirst));
		assertThat(marchDataPoint.getLongValue(), is(22L));

		assertThat(aggregated.hasNext(), is(true));
		DateTime aprilFirst = new DateTime(2014, 4, 1, 0, 0, utc);
		DataPoint aprilDataPoint = aggregated.next();
		assertThat(new DateTime(aprilDataPoint.getTimestamp(), utc), equalTo(aprilFirst));
		assertThat(aprilDataPoint.getLongValue(), is(30L));

		assertThat(aggregated.hasNext(), is(true));
		DateTime mayFirst = new DateTime(2014, 5, 1, 0, 0, utc);
		DataPoint mayMonthDataPoint = aggregated.next();
		assertThat(new DateTime(mayMonthDataPoint.getTimestamp(), utc), equalTo(mayFirst));
		assertThat(mayMonthDataPoint.getLongValue(), is(22L));

		assertThat(aggregated.hasNext(), is(false));
	}

	@Test
	public void test_aggregateByMonthStartAtMidMonthAlignEnd()
	{
		DateTimeZone utc = DateTimeZone.UTC;

		DateTime startDate = new DateTime(2014, 3, 10, 0, 0, utc);
		DateTime stopDate = new DateTime(2014, 5, 23, 0, 0, utc);
		ListDataPointGroup dpGroup = new ListDataPointGroup("range_test");
		for (DateTime iterationDT = startDate;
		     iterationDT.isBefore(stopDate);
		     iterationDT = iterationDT.plusDays(1))
		{
			dpGroup.addDataPoint(new LongDataPoint(iterationDT.getMillis(), 1));
		}

		SumAggregator aggregator = new SumAggregator(new DoubleDataPointFactoryImpl());
		aggregator.setSampling(new Sampling(1, TimeUnit.MONTHS));
		aggregator.setAlignSampling(true);
		aggregator.setAlignEndTime(true);
		aggregator.setStartTime(startDate.getMillis());

		DataPointGroup aggregated = aggregator.aggregate(dpGroup);

		assertThat(aggregated.hasNext(), is(true));
		DateTime marchFirst = new DateTime(2014, 4, 1, 0, 0, utc);
		DataPoint marchDataPoint = aggregated.next();
		assertThat(new DateTime(marchDataPoint.getTimestamp(), utc), equalTo(marchFirst));
		assertThat(marchDataPoint.getLongValue(), is(22L));

		assertThat(aggregated.hasNext(), is(true));
		DateTime aprilFirst = new DateTime(2014, 5, 1, 0, 0, utc);
		DataPoint aprilDataPoint = aggregated.next();
		assertThat(new DateTime(aprilDataPoint.getTimestamp(), utc), equalTo(aprilFirst));
		assertThat(aprilDataPoint.getLongValue(), is(30L));

		assertThat(aggregated.hasNext(), is(true));
		DateTime mayFirst = new DateTime(2014, 6, 1, 0, 0, utc);
		DataPoint mayMonthDataPoint = aggregated.next();
		assertThat(new DateTime(mayMonthDataPoint.getTimestamp(), utc), equalTo(mayFirst));
		assertThat(mayMonthDataPoint.getLongValue(), is(22L));

		assertThat(aggregated.hasNext(), is(false));
	}

	@Test
	public void test_aggregationByWeekAlign()
	{
		DateTimeZone utc = DateTimeZone.UTC;
		DateTime startDate = new DateTime(2015, 1, 2, 1, 11, utc);
		DateTime endDate = new DateTime(2015, 1, 18, 11, 11, utc);

		ListDataPointGroup dpGroup = new ListDataPointGroup("range_test");
		for (DateTime iterationDT = startDate;
		     iterationDT.isBefore(endDate);
		     iterationDT = iterationDT.plusDays(1))
		{
			dpGroup.addDataPoint(new LongDataPoint(iterationDT.getMillis(), 1));
		}

		SumAggregator aggregator = new SumAggregator(new DoubleDataPointFactoryImpl());
		aggregator.setSampling(new Sampling(1, TimeUnit.WEEKS));
		aggregator.setAlignSampling(true);
		aggregator.setAlignStartTime(true);
		aggregator.setStartTime(startDate.getMillis());

		DataPointGroup aggregated = aggregator.aggregate(dpGroup);

		assertThat(aggregated.hasNext(), is(true));
		DateTime firstWeekStart = new DateTime(2014, 12, 29, 0, 0, utc);
		DataPoint firstWeekDataPoint = aggregated.next();
		assertThat(new DateTime(firstWeekDataPoint.getTimestamp(), utc), equalTo(firstWeekStart));
		assertThat(firstWeekDataPoint.getLongValue(), is(3L));

		assertThat(aggregated.hasNext(), is(true));
		DateTime secondWeekStart = new DateTime(2015, 1, 5, 0, 0, utc);
		DataPoint secondWeekDataPoint = aggregated.next();
		assertThat(new DateTime(secondWeekDataPoint.getTimestamp(), utc), equalTo(secondWeekStart));
		assertThat(secondWeekDataPoint.getLongValue(), is(7L));

		assertThat(aggregated.hasNext(), is(true));
		DateTime thirdWeekStart = new DateTime(2015, 1, 12, 0, 0, utc);
		DataPoint thirdWeekDataPoint = aggregated.next();
		assertThat(new DateTime(thirdWeekDataPoint.getTimestamp(), utc), equalTo(thirdWeekStart));
		assertThat(thirdWeekDataPoint.getLongValue(), is(7L));

		assertThat(aggregated.hasNext(), is(false));
	}

	@Test
	public void test_aggregationByWeekAlignEnd()
	{
		DateTimeZone utc = DateTimeZone.UTC;
		DateTime startDate = new DateTime(2015, 1, 2, 1, 11, utc);
		DateTime endDate = new DateTime(2015, 1, 18, 11, 11, utc);

		ListDataPointGroup dpGroup = new ListDataPointGroup("range_test");
		for (DateTime iterationDT = startDate;
		     iterationDT.isBefore(endDate);
		     iterationDT = iterationDT.plusDays(1))
		{
			dpGroup.addDataPoint(new LongDataPoint(iterationDT.getMillis(), 1));
		}

		SumAggregator aggregator = new SumAggregator(new DoubleDataPointFactoryImpl());
		aggregator.setSampling(new Sampling(1, TimeUnit.WEEKS));
		aggregator.setAlignSampling(true);
		aggregator.setAlignEndTime(true);
		aggregator.setStartTime(startDate.getMillis());

		DataPointGroup aggregated = aggregator.aggregate(dpGroup);

		assertThat(aggregated.hasNext(), is(true));
		DateTime firstWeekStart = new DateTime(2015, 1, 5, 0, 0, utc);
		DataPoint firstWeekDataPoint = aggregated.next();
		assertThat(new DateTime(firstWeekDataPoint.getTimestamp(), utc), equalTo(firstWeekStart));
		assertThat(firstWeekDataPoint.getLongValue(), is(3L));

		assertThat(aggregated.hasNext(), is(true));
		DateTime secondWeekStart = new DateTime(2015, 1, 12, 0, 0, utc);
		DataPoint secondWeekDataPoint = aggregated.next();
		assertThat(new DateTime(secondWeekDataPoint.getTimestamp(), utc), equalTo(secondWeekStart));
		assertThat(secondWeekDataPoint.getLongValue(), is(7L));

		assertThat(aggregated.hasNext(), is(true));
		DateTime thirdWeekStart = new DateTime(2015, 1, 19, 0, 0, utc);
		DataPoint thirdWeekDataPoint = aggregated.next();
		assertThat(new DateTime(thirdWeekDataPoint.getTimestamp(), utc), equalTo(thirdWeekStart));
		assertThat(thirdWeekDataPoint.getLongValue(), is(7L));

		assertThat(aggregated.hasNext(), is(false));
	}

	@Test
	public void test_aggregationByHourOverMarch_dst()
	{
		ListDataPointGroup group = new ListDataPointGroup("March");
		DateTimeZone paris = DateTimeZone.forID("Europe/Paris");

		for (DateTime hour = new DateTime(2014, 3, 1, 0, 0, paris); // 1st of March
		     hour.isBefore(new DateTime(2014, 4, 1, 0, 0, paris)); // 1st of April
		     hour = hour.plusHours(1)
				)
		{
			group.addDataPoint(new LongDataPoint(hour.getMillis(), 1L));
		}


		SumAggregator aggregator = new SumAggregator(new DoubleDataPointFactoryImpl());
		aggregator.setTimeZone(paris);
		aggregator.setSampling(new Sampling(1, TimeUnit.MONTHS));
		aggregator.setAlignSampling(true);

		DataPointGroup hourCount = aggregator.aggregate(group);
		assert hourCount.hasNext();
		// 31 * 24 - 1 = 743 hours in March
		assertThat(hourCount.next().getLongValue(), is(743L));
		assertThat(hourCount.hasNext(), is(false));
	}

	@Test
	public void test_aggregationByHourOverOctober_dst()
	{
		ListDataPointGroup group = new ListDataPointGroup("October");
		DateTimeZone paris = DateTimeZone.forID("Europe/Paris");

		for (DateTime hour = new DateTime(2014, 10, 1, 0, 0, paris); // 1st of October
		     hour.isBefore(new DateTime(2014, 11, 1, 0, 0, paris)); // 1st of November
		     hour = hour.plusHours(1))
		{
			group.addDataPoint(new LongDataPoint(hour.getMillis(), 1L));
		}


		SumAggregator aggregator = new SumAggregator(new DoubleDataPointFactoryImpl());
		aggregator.setTimeZone(paris);
		aggregator.setSampling(new Sampling(1, TimeUnit.MONTHS));
		aggregator.setAlignSampling(true);

		DataPointGroup hourCount = aggregator.aggregate(group);
		assertThat(hourCount.hasNext(), is(true));
		// 31 * 24 + 1 = 745 hours in October
		assertThat(hourCount.next().getLongValue(), is(745L));

		assertThat(hourCount.hasNext(), is(false));
	}


	@Test
	public void test_aggregationByYearOverLeapYears()
	{
		DateTimeZone utc = DateTimeZone.UTC;
		DateTime startDate = new DateTime(2010, 1, 1, 0, 0, utc);
		DateTime endDate = new DateTime(2014, 1, 1, 0, 0, utc);

		ListDataPointGroup dpGroup = new ListDataPointGroup("range_test");
		for (DateTime iterationDT = startDate;
		     iterationDT.isBefore(endDate);
		     iterationDT = iterationDT.plusDays(1))
		{
			dpGroup.addDataPoint(new LongDataPoint(iterationDT.getMillis(), 1));
		}

		SumAggregator aggregator = new SumAggregator(new DoubleDataPointFactoryImpl());
		aggregator.setSampling(new Sampling(1, TimeUnit.YEARS));
		aggregator.setAlignSampling(false);

		DataPointGroup dayCount = aggregator.aggregate(dpGroup);

		assertThat(dayCount.hasNext(), is(true));
		assertThat(dayCount.next().getLongValue(), is(365L)); // 2010

		assertThat(dayCount.hasNext(), is(true));
		assertThat(dayCount.next().getLongValue(), is(365L)); // 2011

		assertThat(dayCount.hasNext(), is(true));
		assertThat(dayCount.next().getLongValue(), is(366L)); // 2012

		assertThat(dayCount.hasNext(), is(true));
		assertThat(dayCount.next().getLongValue(), is(365L)); // 2013

		assertThat(dayCount.hasNext(), is(false));
	}

	@Test
	public void test_aggregationByDay() throws Exception
	{
		DateTimeZone utc = DateTimeZone.UTC;
		DateTime startDate = new DateTime(2014, 1, 1, 0, 0, utc);
		DateTime endDate = new DateTime(2014, 1, 3, 3, 0, utc);
		ListDataPointGroup group = new ListDataPointGroup("aggregationByDay");

		for (DateTime hour = startDate;
		     hour.isBefore(endDate);
		     hour = hour.plusHours(1))
		{
			group.addDataPoint(new LongDataPoint(hour.getMillis(), 1L));
		}

		SumAggregator aggregator = new SumAggregator(new DoubleDataPointFactoryImpl());
		aggregator.setSampling(new Sampling(1, TimeUnit.DAYS));
		aggregator.setAlignSampling(false);

		DataPointGroup dayCount = aggregator.aggregate(group);

		assertThat(dayCount.hasNext(), is(true));
		assertThat(dayCount.next().getLongValue(), is(24L));

		assertThat(dayCount.hasNext(), is(true));
		assertThat(dayCount.next().getLongValue(), is(24L));

		assertThat(dayCount.hasNext(), is(true));
		assertThat(dayCount.next().getLongValue(), is(3L));

		assertThat(dayCount.hasNext(), is(false));
	}

	@Test
	public void test_aggregationByHour() throws Exception
	{
		DateTimeZone utc = DateTimeZone.UTC;
		DateTime startDate = new DateTime(2014, 1, 1, 0, 0, utc);
		DateTime endDate = new DateTime(2014, 1, 1, 2, 20, utc);
		ListDataPointGroup group = new ListDataPointGroup("aggregationByDay");

		for (DateTime minute = startDate;
		     minute.isBefore(endDate);
		     minute = minute.plusMinutes(10))
		{
			group.addDataPoint(new LongDataPoint(minute.getMillis(), 1L));
		}

		SumAggregator aggregator = new SumAggregator(new DoubleDataPointFactoryImpl());
		aggregator.setSampling(new Sampling(1, TimeUnit.HOURS));
		aggregator.setAlignSampling(false);

		DataPointGroup dayCount = aggregator.aggregate(group);

		assertThat(dayCount.hasNext(), is(true));
		assertThat(dayCount.next().getLongValue(), is(6L));

		assertThat(dayCount.hasNext(), is(true));
		assertThat(dayCount.next().getLongValue(), is(6L));

		assertThat(dayCount.hasNext(), is(true));
		assertThat(dayCount.next().getLongValue(), is(2L));

		assertThat(dayCount.hasNext(), is(false));
	}

	@Test
	public void test_aggregationByMinute() throws Exception
	{
		DateTimeZone utc = DateTimeZone.UTC;
		DateTime startDate = new DateTime(2014, 1, 1, 0, 0, 0, utc);
		DateTime endDate = new DateTime(2014, 1, 1, 0, 2, 20, utc);
		ListDataPointGroup group = new ListDataPointGroup("aggregationByDay");

		for (DateTime second = startDate;
		     second.isBefore(endDate);
		     second = second.plusSeconds(10))
		{
			group.addDataPoint(new LongDataPoint(second.getMillis(), 1L));
		}

		SumAggregator aggregator = new SumAggregator(new DoubleDataPointFactoryImpl());
		aggregator.setSampling(new Sampling(1, TimeUnit.MINUTES));
		aggregator.setAlignSampling(false);

		DataPointGroup dayCount = aggregator.aggregate(group);

		assertThat(dayCount.hasNext(), is(true));
		assertThat(dayCount.next().getLongValue(), is(6L));

		assertThat(dayCount.hasNext(), is(true));
		assertThat(dayCount.next().getLongValue(), is(6L));

		assertThat(dayCount.hasNext(), is(true));
		assertThat(dayCount.next().getLongValue(), is(2L));
		assertThat(dayCount.hasNext(), is(false));
	}

	@Test
	public void test_aggregationBySecond() throws Exception
	{
		DateTimeZone utc = DateTimeZone.UTC;
		DateTime startDate = new DateTime(2014, 1, 1, 0, 0, 0, 0, utc);
		DateTime endDate = new DateTime(2014, 1, 1, 0, 0, 2, 200, utc);
		ListDataPointGroup group = new ListDataPointGroup("aggregationByDay");

		for (DateTime milliSecond = startDate;
		     milliSecond.isBefore(endDate);
		     milliSecond = milliSecond.plus(100))
		{
			group.addDataPoint(new LongDataPoint(milliSecond.getMillis(), 1L));
		}

		SumAggregator aggregator = new SumAggregator(new DoubleDataPointFactoryImpl());
		aggregator.setSampling(new Sampling(1, TimeUnit.SECONDS));
		aggregator.setAlignSampling(false);

		DataPointGroup dayCount = aggregator.aggregate(group);

		assertThat(dayCount.hasNext(), is(true));
		assertThat(dayCount.next().getLongValue(), is(10L));

		assertThat(dayCount.hasNext(), is(true));
		assertThat(dayCount.next().getLongValue(), is(10L));

		assertThat(dayCount.hasNext(), is(true));
		assertThat(dayCount.next().getLongValue(), is(2L));
		assertThat(dayCount.hasNext(), is(false));
	}


	@Test
	public void test_aggregationByMilliSecond() throws Exception
	{
		DateTimeZone utc = DateTimeZone.UTC;
		DateTime startDate = new DateTime(2014, 1, 1, 0, 0, 0, 0, utc);
		DateTime endDate = new DateTime(2014, 1, 1, 0, 0, 0, 3, utc);
		ListDataPointGroup group = new ListDataPointGroup("aggregationByDay");

		for (DateTime milliSecond = startDate;
		     milliSecond.isBefore(endDate);
		     milliSecond = milliSecond.plus(1))
		{
			group.addDataPoint(new LongDataPoint(milliSecond.getMillis(), 1L));
			group.addDataPoint(new LongDataPoint(milliSecond.getMillis(), 1L));
		}

		SumAggregator aggregator = new SumAggregator(new DoubleDataPointFactoryImpl());
		aggregator.setSampling(new Sampling(1, TimeUnit.MILLISECONDS));
		aggregator.setAlignSampling(false);
		aggregator.setStartTime(startDate.getMillis());

		DataPointGroup dayCount = aggregator.aggregate(group);

		assertThat(dayCount.hasNext(), is(true));
		DataPoint firstMillis = dayCount.next();
		assertThat(new DateTime(firstMillis.getTimestamp(), utc), equalTo(startDate));
		assertThat(firstMillis.getLongValue(), is(2L));

		assertThat(dayCount.hasNext(), is(true));
		DataPoint secondMillis = dayCount.next();
		assertThat(new DateTime(secondMillis.getTimestamp(), utc), equalTo(startDate.plus(1)));
		assertThat(secondMillis.getLongValue(), is(2L));

		assertThat(dayCount.hasNext(), is(true));
		DataPoint thirdMillis = dayCount.next();
		assertThat(new DateTime(thirdMillis.getTimestamp(), utc), equalTo(startDate.plus(2)));
		assertThat(thirdMillis.getLongValue(), is(2L));

		assertThat(dayCount.hasNext(), is(false));
	}

	/**
	 This makes test makes the RangeAggregator do time calculations larger than an
	 int.

	 @throws Exception
	 */
	@Test
	public void test_aggregationByMilliSecondOverLongRange() throws Exception
	{
		DateTimeZone utc = DateTimeZone.UTC;
		DateTime startDate = new DateTime(2014, 1, 1, 0, 0, 0, 0, utc);
		DateTime endDate = new DateTime(2014, 3, 1, 0, 0, 0, 0, utc);
		ListDataPointGroup group = new ListDataPointGroup("aggregationByDay");

		for (DateTime milliSecond = startDate;
		     milliSecond.isBefore(endDate);
		     milliSecond = milliSecond.plus(10000))
		{
			group.addDataPoint(new LongDataPoint(milliSecond.getMillis(), 1L));
			group.addDataPoint(new LongDataPoint(milliSecond.getMillis(), 1L));
		}

		SumAggregator aggregator = new SumAggregator(new DoubleDataPointFactoryImpl());
		aggregator.setSampling(new Sampling(1, TimeUnit.MILLISECONDS));
		aggregator.setAlignSampling(false);
		aggregator.setStartTime(startDate.getMillis());

		DataPointGroup dpg = aggregator.aggregate(group);

		while (dpg.hasNext())
			dpg.next();


	}
}
