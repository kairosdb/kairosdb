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
import org.kairosdb.core.datastore.Sampling;
import org.kairosdb.core.datastore.TimeUnit;
import org.kairosdb.testing.ListDataPointGroup;

import java.util.Calendar;
import java.util.TimeZone;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class RangeAggregatorTest {
    @Test
    public void test_yearRange() {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        ListDataPointGroup dpGroup = new ListDataPointGroup("range_test");

        for (int I = 0; I < 12; I++) {
            cal.clear();
            cal.set(2012, I, 1, 1, 1, 1);
            dpGroup.addDataPoint(new LongDataPoint(cal.getTimeInMillis(), 1));
        }

        SumAggregator agg = new SumAggregator(new DoubleDataPointFactoryImpl());
        agg.setSampling(new Sampling(1, TimeUnit.YEARS));
        agg.setAlignSampling(true);
        cal.clear();
        cal.set(2012, Calendar.JANUARY, 0, 0, 0, 0);
        agg.setStartTime(cal.getTimeInMillis());

        DataPointGroup dpg = agg.aggregate(dpGroup);

        assertThat(dpg.hasNext(), is(true));
        assertThat(dpg.next().getLongValue(), is(12L));

        assertThat(dpg.hasNext(), is(false));
    }

    @Test
    public void test_monthRange() {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        ListDataPointGroup dpGroup = new ListDataPointGroup("range_test");

        for (int I = 1; I <= 32; I++) {
            cal.clear();
            cal.set(2012, Calendar.JANUARY, I, 1, 1, 1);
            dpGroup.addDataPoint(new LongDataPoint(cal.getTimeInMillis(), 1));
        }

        SumAggregator agg = new SumAggregator(new DoubleDataPointFactoryImpl());
        agg.setSampling(new Sampling(1, TimeUnit.MONTHS));
        agg.setAlignSampling(false);
        cal.clear();
        cal.set(2012, Calendar.JANUARY, 1, 1, 1, 1);
        agg.setStartTime(cal.getTimeInMillis());

        DataPointGroup dpg = agg.aggregate(dpGroup);

		/*while (dpg.hasNext())
			System.out.println(dpg.next().getLongValue());*/

        assertThat(dpg.hasNext(), is(true));
        assertThat(dpg.next().getLongValue(), is(31L));

        assertThat(dpg.hasNext(), is(true));
        assertThat(dpg.next().getLongValue(), is(1L));

        assertThat(dpg.hasNext(), is(false));
    }

    @Test
    public void test_mulitpleMonths() {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        ListDataPointGroup dpGroup = new ListDataPointGroup("range_test");

        for (int I = 1; I <= 70; I++) {
            cal.clear();
            cal.set(2012, Calendar.JANUARY, I, 1, 1, 1);
            dpGroup.addDataPoint(new LongDataPoint(cal.getTimeInMillis(), 1));
        }

        SumAggregator agg = new SumAggregator(new DoubleDataPointFactoryImpl());
        agg.setSampling(new Sampling(2, TimeUnit.MONTHS));
        agg.setAlignSampling(false);
        cal.clear();
        cal.set(2012, Calendar.JANUARY, 1, 0, 0, 0);
        System.out.println(cal.getTime());
        agg.setStartTime(cal.getTimeInMillis());

        DataPointGroup dpg = agg.aggregate(dpGroup);

		/*while (dpg.hasNext())
			System.out.println(dpg.next().getLongValue());*/

        assertThat(dpg.hasNext(), is(true));
        assertThat(dpg.next().getLongValue(), is(60L));

        assertThat(dpg.hasNext(), is(true));
        assertThat(dpg.next().getLongValue(), is(10L));

        assertThat(dpg.hasNext(), is(false));
    }

    @Test
    public void test_midMonthStart() {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        ListDataPointGroup dpGroup = new ListDataPointGroup("range_test");

        for (int I = 1; I <= 32; I++) {
            cal.clear();
            cal.set(2012, 0, I + 15, 1, 1, 1);
            dpGroup.addDataPoint(new LongDataPoint(cal.getTimeInMillis(), 1));
        }

        SumAggregator agg = new SumAggregator(new DoubleDataPointFactoryImpl());
        agg.setSampling(new Sampling(1, TimeUnit.MONTHS));
        cal.clear();
        cal.set(2012, 0, 16, 0, 0, 0);
        agg.setStartTime(cal.getTimeInMillis());

        DataPointGroup dpg = agg.aggregate(dpGroup);

		/*while (dpg.hasNext())
			System.out.println(dpg.next().getLongValue());*/

        assertThat(dpg.hasNext(), is(true));
        assertThat(dpg.next().getLongValue(), is(31L));

        assertThat(dpg.hasNext(), is(true));
        assertThat(dpg.next().getLongValue(), is(1L));

        assertThat(dpg.hasNext(), is(false));
    }

    @Test
    public void test_alignOnWeek() {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        ListDataPointGroup dpGroup = new ListDataPointGroup("range_test");

        for (int I = 1; I <= 32; I++) {
            cal.clear();
            cal.set(2012, 0, I + 15, 1, 1, 1);
            dpGroup.addDataPoint(new LongDataPoint(cal.getTimeInMillis(), 1));
        }

        SumAggregator agg = new SumAggregator(new DoubleDataPointFactoryImpl());
        agg.setSampling(new Sampling(1, TimeUnit.WEEKS));
        agg.setAlignSampling(true);
        cal.clear();
        cal.set(2012, 0, 16, 0, 0, 0);
        agg.setStartTime(cal.getTimeInMillis());

        //Just making sure the alignment doesn't blow up
        DataPointGroup dpg = agg.aggregate(dpGroup);
    }

    @Test
    public void test_dstMarch() {
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
        aggregator.setSampling(new Sampling(1, TimeUnit.MONTHS, paris));
        aggregator.setAlignSampling(true);

        DataPointGroup hourCount = aggregator.aggregate(group);
        assert hourCount.hasNext();
        // 31 * 24 - 1 = 743 hours in March
        assertThat(hourCount.next().getDoubleValue(), is((double) 743));
    }

    @Test
    public void test_dstOctober() {
        ListDataPointGroup group = new ListDataPointGroup("October");
        DateTimeZone paris = DateTimeZone.forID("Europe/Paris");

        for (DateTime hour = new DateTime(2014, 10, 1, 0, 0, paris); // 1st of October
             hour.isBefore(new DateTime(2014, 11, 1, 0, 0, paris)); // 1st of November
             hour = hour.plusHours(1))
        {
            group.addDataPoint(new LongDataPoint(hour.getMillis(), 1L));
        }


        SumAggregator aggregator = new SumAggregator(new DoubleDataPointFactoryImpl());
        aggregator.setSampling(new Sampling(1, TimeUnit.MONTHS, paris));
        aggregator.setAlignSampling(true);

        DataPointGroup hourCount = aggregator.aggregate(group);
        assertThat(hourCount.hasNext(), is(true));
        // 31 * 24 + 1 = 745 hours in October
        assertThat(hourCount.next().getDoubleValue(), is((double) 745));
    }

    @Test
    public void test_februaryRange() {
        ListDataPointGroup group = new ListDataPointGroup("4years");
        DateTimeZone utc = DateTimeZone.UTC;

        for (DateTime day = new DateTime(2010, 1, 1, 0, 0, utc);
             day.isBefore(new DateTime(2014, 1, 1, 0, 0, utc));
             day = day.plusDays(1))
        {
            group.addDataPoint(new LongDataPoint(day.getMillis(), 1L));
        }

        SumAggregator aggregator = new SumAggregator(new DoubleDataPointFactoryImpl());
        aggregator.setSampling(new Sampling(1, TimeUnit.MONTHS, utc));
        aggregator.setAlignSampling(true);

        DataPointGroup dayCount = aggregator.aggregate(group);
        while (dayCount.hasNext()) {
            DataPoint dataPoint = dayCount.next();
            DateTime time = new DateTime(dataPoint.getTimestamp(), utc);
            if (time.getMonthOfYear() == 2) {
                if (time.year().isLeap())
                    assertThat(dataPoint.getLongValue(), is(29L));
                else
                    assertThat(dataPoint.getLongValue(), is(28L));
            }
        }
    }

    @Test
    public void test_leapYears()
    {
        ListDataPointGroup group = new ListDataPointGroup("4years");
        DateTimeZone utc = DateTimeZone.UTC;

        for (DateTime day = new DateTime(2010, 1, 1, 0, 0, utc);
             day.isBefore(new DateTime(2014, 1, 1, 0, 0, utc));
             day = day.plusDays(1))
        {
            group.addDataPoint(new LongDataPoint(day.getMillis(), 1L));
        }

        SumAggregator aggregator = new SumAggregator(new DoubleDataPointFactoryImpl());
        aggregator.setSampling(new Sampling(1, TimeUnit.YEARS, utc));
        aggregator.setAlignSampling(true);

        DataPointGroup dayCount = aggregator.aggregate(group);
        while (dayCount.hasNext()) {
            DataPoint dataPoint = dayCount.next();
            DateTime time = new DateTime(dataPoint.getTimestamp(), utc);
            assertThat(time.getDayOfMonth(), is(1));
            assertThat(time.getMonthOfYear(), is(1));
            if (time.year().isLeap())
                assertThat(dataPoint.getLongValue(), is(366L));
            else
                assertThat(dataPoint.getLongValue(), is(365L));
        }
    }
}
