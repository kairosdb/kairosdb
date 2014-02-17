//
// RangeAggregatorTest.java
//
// Copyright 2013, NextPage Inc. All rights reserved.
//

package org.kairosdb.core.aggregator;

import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.Sampling;
import org.kairosdb.core.datastore.TimeUnit;
import org.kairosdb.testing.ListDataPointGroup;

import java.util.Calendar;
import java.util.TimeZone;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class RangeAggregatorTest
{
	@Test
	public void test_yearRange()
	{
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		ListDataPointGroup dpGroup = new ListDataPointGroup("range_test");

		for (int I = 0; I < 12; I++)
		{
			cal.clear();
			cal.set(2012, I, 1, 1, 1, 1);
			dpGroup.addDataPoint(new DataPoint(cal.getTimeInMillis(), 1));
		}

		SumAggregator agg = new SumAggregator();
		agg.setSampling(new Sampling(1, TimeUnit.YEARS));
		agg.setAlignSampling(true);
		cal.clear();
		cal.set(2012, 0, 0, 0, 0, 0);
		agg.setStartTime(cal.getTimeInMillis());

		DataPointGroup dpg = agg.aggregate(dpGroup);

		assertThat(dpg.hasNext(), is(true));
		assertThat(dpg.next().getLongValue(), is(12L));

		assertThat(dpg.hasNext(), is(false));
	}


	@Test
	public void test_monthRange()
	{
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		ListDataPointGroup dpGroup = new ListDataPointGroup("range_test");

		for (int I = 1; I <= 32; I++)
		{
			cal.clear();
			cal.set(2012, Calendar.JANUARY, I, 1, 1, 1);
			dpGroup.addDataPoint(new DataPoint(cal.getTimeInMillis(), 1));
		}

		SumAggregator agg = new SumAggregator();
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
	public void test_mulitpleMonths()
	{
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		ListDataPointGroup dpGroup = new ListDataPointGroup("range_test");

		for (int I = 1; I <= 70; I++)
		{
			cal.clear();
			cal.set(2012, Calendar.JANUARY, I, 1, 1, 1);
			dpGroup.addDataPoint(new DataPoint(cal.getTimeInMillis(), 1));
		}

		SumAggregator agg = new SumAggregator();
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
	public void test_midMonthStart()
	{
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		ListDataPointGroup dpGroup = new ListDataPointGroup("range_test");

		for (int I = 1; I <= 32; I++)
		{
			cal.clear();
			cal.set(2012, 0, I+15, 1, 1, 1);
			dpGroup.addDataPoint(new DataPoint(cal.getTimeInMillis(), 1));
		}

		SumAggregator agg = new SumAggregator();
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
	public void test_alignOnWeek()
	{
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		ListDataPointGroup dpGroup = new ListDataPointGroup("range_test");

		for (int I = 1; I <= 32; I++)
		{
			cal.clear();
			cal.set(2012, 0, I+15, 1, 1, 1);
			dpGroup.addDataPoint(new DataPoint(cal.getTimeInMillis(), 1));
		}

		SumAggregator agg = new SumAggregator();
		agg.setSampling(new Sampling(1, TimeUnit.WEEKS));
		agg.setAlignSampling(true);
		cal.clear();
		cal.set(2012, 0, 16, 0, 0, 0);
		agg.setStartTime(cal.getTimeInMillis());

		//Just making sure the alignment doesn't blow up
		DataPointGroup dpg = agg.aggregate(dpGroup);
	}
}
