package org.kairosdb.rollup;

import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.kairosdb.core.aggregator.RangeAggregator;
import org.kairosdb.core.aggregator.Sampling;
import org.kairosdb.core.aggregator.SumAggregator;
import org.kairosdb.core.datapoints.DoubleDataPointFactoryImpl;
import org.kairosdb.core.datastore.TimeUnit;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class RollupUtilTest
{
	@Test
	public void testGetSamplingPeriodsAlignedToUnit_milliseconds()
	{
		long start = ZonedDateTime.of(2019, 3, 2, 7, 12, 7, 2000000, ZoneOffset.UTC).toInstant().toEpochMilli();
		long end = ZonedDateTime.of(2019, 3, 2, 7, 12, 7, 9000000, ZoneOffset.UTC).toInstant().toEpochMilli();
		RangeAggregator aggregator = createAggregator(new Sampling(5, TimeUnit.MILLISECONDS));
		List<SamplingPeriod> samplingPeriods = RollupUtil.getSamplingPeriodsAlignedToUnit(aggregator, start, end, DateTimeZone.UTC);

		assertThat(samplingPeriods.size(), equalTo(2));
		assertThat(samplingPeriods.get(0).getStartTime(), equalTo(time(2019, 3, 2, 7, 12, 7, 0)));
		assertThat(samplingPeriods.get(0).getEndTime(), equalTo(time(2019, 3, 2, 7, 12, 7, 5)));
		assertThat(samplingPeriods.get(1).getStartTime(), equalTo(time(2019, 3, 2, 7, 12, 7, 5)));
		assertThat(samplingPeriods.get(1).getEndTime(), equalTo(time(2019, 3, 2, 7, 12, 7, 10)));
	}

	@Test
	public void testGetSamplingPeriodsAlignedToUnit_seconds()
	{
		long start = ZonedDateTime.of(2019, 3, 2, 7, 12, 7, 0, ZoneOffset.UTC).toInstant().toEpochMilli();
		long end = ZonedDateTime.of(2019, 3, 2, 7, 12, 14, 5000000, ZoneOffset.UTC).toInstant().toEpochMilli();
		RangeAggregator aggregator = createAggregator(new Sampling(2, TimeUnit.SECONDS));
		List<SamplingPeriod> samplingPeriods = RollupUtil.getSamplingPeriodsAlignedToUnit(aggregator, start, end, DateTimeZone.UTC);

		assertThat(samplingPeriods.size(), equalTo(5));
		assertThat(samplingPeriods.get(0).getStartTime(), equalTo(time(2019, 3, 2, 7, 12, 6, 0)));
		assertThat(samplingPeriods.get(0).getEndTime(), equalTo(time(2019, 3, 2, 7, 12, 8, 0)));
		assertThat(samplingPeriods.get(1).getStartTime(), equalTo(time(2019, 3, 2, 7, 12, 8, 0)));
		assertThat(samplingPeriods.get(1).getEndTime(), equalTo(time(2019, 3, 2, 7, 12, 10, 0)));
		assertThat(samplingPeriods.get(2).getStartTime(), equalTo(time(2019, 3, 2, 7, 12, 10, 0)));
		assertThat(samplingPeriods.get(2).getEndTime(), equalTo(time(2019, 3, 2, 7, 12, 12, 0)));
		assertThat(samplingPeriods.get(3).getStartTime(), equalTo(time(2019, 3, 2, 7, 12, 12, 0)));
		assertThat(samplingPeriods.get(3).getEndTime(), equalTo(time(2019, 3, 2, 7, 12, 14, 0)));
		assertThat(samplingPeriods.get(4).getStartTime(), equalTo(time(2019, 3, 2, 7, 12, 14, 0)));
	}

	@Test
	public void testGetSamplingPeriodsAlignedToUnit_minute()
	{
		long start = ZonedDateTime.of(2019, 3, 2, 7, 5, 9, 0,ZoneOffset.UTC).toInstant().toEpochMilli();
		long end = ZonedDateTime.of(2019, 3, 2, 8, 0, 40, 0, ZoneOffset.UTC).toInstant().toEpochMilli();
		RangeAggregator aggregator = createAggregator(new Sampling(10, TimeUnit.MINUTES));
		List<SamplingPeriod> samplingPeriods = RollupUtil.getSamplingPeriodsAlignedToUnit(aggregator, start, end, DateTimeZone.UTC);

		assertThat(samplingPeriods.size(), equalTo(7));
		assertThat(samplingPeriods.get(0).getStartTime(), equalTo(time(2019, 3, 2, 7, 0, 0)));
		assertThat(samplingPeriods.get(0).getEndTime(), equalTo(time(2019, 3, 2, 7, 10, 0)));
		assertThat(samplingPeriods.get(1).getStartTime(), equalTo(time(2019, 3, 2, 7, 10, 0)));
		assertThat(samplingPeriods.get(1).getEndTime(), equalTo(time(2019, 3, 2, 7, 20, 0)));
		assertThat(samplingPeriods.get(2).getStartTime(), equalTo(time(2019, 3, 2, 7, 20, 0)));
		assertThat(samplingPeriods.get(2).getEndTime(), equalTo(time(2019, 3, 2, 7, 30, 0)));
		assertThat(samplingPeriods.get(3).getStartTime(), equalTo(time(2019, 3, 2, 7, 30, 0)));
		assertThat(samplingPeriods.get(3).getEndTime(), equalTo(time(2019, 3, 2, 7, 40, 0)));
		assertThat(samplingPeriods.get(4).getStartTime(), equalTo(time(2019, 3, 2, 7, 40, 0)));
		assertThat(samplingPeriods.get(4).getEndTime(), equalTo(time(2019, 3, 2, 7, 50, 0)));
		assertThat(samplingPeriods.get(5).getStartTime(), equalTo(time(2019, 3, 2, 7, 50, 0)));
		assertThat(samplingPeriods.get(5).getEndTime(), equalTo(time(2019, 3, 2, 8, 0, 0)));
		assertThat(samplingPeriods.get(6).getStartTime(), equalTo(time(2019, 3, 2, 8, 0, 0)));
		assertThat(samplingPeriods.get(6).getEndTime(), equalTo(time(2019, 3, 2, 8, 10, 0)));
	}

	@Test
	public void testGetSamplingPeriodsAlignedToUnit_hour()
	{
		long start = ZonedDateTime.of(2019, 3, 2, 7, 5, 0, 0, ZoneOffset.UTC).toInstant().toEpochMilli();
		long end = ZonedDateTime.of(2019, 3, 2, 20, 5, 0, 0, ZoneOffset.UTC).toInstant().toEpochMilli();
		RangeAggregator aggregator = createAggregator(new Sampling(5, TimeUnit.HOURS));
		List<SamplingPeriod> samplingPeriods = RollupUtil.getSamplingPeriodsAlignedToUnit(aggregator, start, end, DateTimeZone.UTC);

		assertThat(samplingPeriods.size(), equalTo(3));
		assertThat(samplingPeriods.get(0).getStartTime(), equalTo(time(2019, 3, 2, 7, 0)));
		assertThat(samplingPeriods.get(0).getEndTime(), equalTo(time(2019, 3, 2, 12, 0)));
		assertThat(samplingPeriods.get(1).getStartTime(), equalTo(time(2019, 3, 2, 12, 0)));
		assertThat(samplingPeriods.get(1).getEndTime(), equalTo(time(2019, 3, 2, 17, 0)));
		assertThat(samplingPeriods.get(2).getStartTime(), equalTo(time(2019, 3, 2, 17, 0)));
		assertThat(samplingPeriods.get(2).getEndTime(), equalTo(time(2019, 3, 2, 22, 0)));
	}

	@Test
	public void testGetSamplingPeriodsAlignedToUnit_day()
	{
		long start = ZonedDateTime.of(2019, 3, 2, 7, 5, 0, 0, ZoneOffset.UTC).toInstant().toEpochMilli();
		long end = ZonedDateTime.of(2019, 3, 15, 14, 12, 0, 0, ZoneOffset.UTC).toInstant().toEpochMilli();
		RangeAggregator aggregator = createAggregator(new Sampling(5, TimeUnit.DAYS));
		List<SamplingPeriod> samplingPeriods = RollupUtil.getSamplingPeriodsAlignedToUnit(aggregator, start, end, DateTimeZone.UTC);

		assertThat(samplingPeriods.size(), equalTo(4));
		assertThat(samplingPeriods.get(0).getStartTime(), equalTo(time(2019, 2, 28, 0)));
		assertThat(samplingPeriods.get(0).getEndTime(), equalTo(time(2019, 3, 5, 0)));
		assertThat(samplingPeriods.get(1).getStartTime(), equalTo(time(2019, 3, 5, 0)));
		assertThat(samplingPeriods.get(1).getEndTime(), equalTo(time(2019, 3, 10, 0)));
		assertThat(samplingPeriods.get(2).getStartTime(), equalTo(time(2019, 3, 10, 0)));
		assertThat(samplingPeriods.get(2).getEndTime(), equalTo(time(2019, 3, 15, 0)));
		assertThat(samplingPeriods.get(3).getStartTime(), equalTo(time(2019, 3, 15, 0)));
		assertThat(samplingPeriods.get(3).getEndTime(), equalTo(time(2019, 3, 20, 0)));
	}

	@Test
	public void testGetSamplingPeriodsAlignedToUnit_month()
	{
		long start = ZonedDateTime.of(2019, 3, 2, 7, 5, 7, 12, ZoneOffset.UTC).toInstant().toEpochMilli();
		long end = ZonedDateTime.of(2019, 10, 9, 14, 12, 9, 22, ZoneOffset.UTC).toInstant().toEpochMilli();
		RangeAggregator aggregator = createAggregator(new Sampling(5, TimeUnit.MONTHS));
		List<SamplingPeriod> samplingPeriods = RollupUtil.getSamplingPeriodsAlignedToUnit(aggregator, start, end, DateTimeZone.UTC);

		assertThat(samplingPeriods.size(), equalTo(2));
		assertThat(samplingPeriods.get(0).getStartTime(), equalTo(time(2019, 3, 1, 0)));
		assertThat(samplingPeriods.get(0).getEndTime(), equalTo(time(2019, 8, 1, 0)));
		assertThat(samplingPeriods.get(1).getStartTime(), equalTo(time(2019, 8, 1, 0)));
		assertThat(samplingPeriods.get(1).getEndTime(), equalTo(time(2020, 1, 1, 0)));
	}

	@Test
	public void testGetSamplingPeriodsAlignedToUnit_year()
	{
		long start = ZonedDateTime.of(2003, 3, 2, 7, 5, 7, 12, ZoneOffset.UTC).toInstant().toEpochMilli();
		long end = ZonedDateTime.of(2010, 10, 9, 14, 12, 9, 22, ZoneOffset.UTC).toInstant().toEpochMilli();
		RangeAggregator aggregator = createAggregator(new Sampling(2, TimeUnit.YEARS));
		List<SamplingPeriod> samplingPeriods = RollupUtil.getSamplingPeriodsAlignedToUnit(aggregator, start, end, DateTimeZone.UTC);

		assertThat(samplingPeriods.size(), equalTo(5));
		assertThat(samplingPeriods.get(0).getStartTime(), equalTo(time(2002, 1, 1, 0)));
		assertThat(samplingPeriods.get(0).getEndTime(), equalTo(time(2004, 1, 1, 0)));
		assertThat(samplingPeriods.get(1).getStartTime(), equalTo(time(2004, 1, 1, 0)));
		assertThat(samplingPeriods.get(1).getEndTime(), equalTo(time(2006, 1, 1, 0)));
		assertThat(samplingPeriods.get(2).getStartTime(), equalTo(time(2006, 1, 1, 0)));
		assertThat(samplingPeriods.get(2).getEndTime(), equalTo(time(2008, 1, 1, 0)));
		assertThat(samplingPeriods.get(3).getStartTime(), equalTo(time(2008, 1, 1, 0)));
		assertThat(samplingPeriods.get(3).getEndTime(), equalTo(time(2010, 1, 1, 0)));
		assertThat(samplingPeriods.get(4).getStartTime(), equalTo(time(2010, 1, 1, 0)));
		assertThat(samplingPeriods.get(4).getEndTime(), equalTo(time(2012, 1, 1, 0)));
	}

	private static RangeAggregator createAggregator(Sampling sampling) {
		RangeAggregator aggregator = new SumAggregator(new DoubleDataPointFactoryImpl());
		aggregator.setSampling(sampling);
		aggregator.setTimeZone(DateTimeZone.UTC);
		aggregator.init();
		return aggregator;
	}

	private static long time(int year, int month, int day, int hour)
	{
		return ZonedDateTime.of(year, month, day, hour, 0, 0, 0, ZoneOffset.UTC).toInstant().toEpochMilli();
	}

	private static long time(int year, int month, int day, int hour, int minute)
	{
		return ZonedDateTime.of(year, month, day, hour, minute, 0, 0, ZoneOffset.UTC).toInstant().toEpochMilli();
	}

	private static long time(int year, int month, int day, int hour, int minute, int second)
	{
		return ZonedDateTime.of(year, month, day, hour, minute, second, 0, ZoneOffset.UTC).toInstant().toEpochMilli();
	}

	private static long time(int year, int month, int day, int hour, int minute, int second, int millisecond)
	{
		return ZonedDateTime.of(year, month, day, hour, minute, second, millisecond * 1000000, ZoneOffset.UTC).toInstant().toEpochMilli();
	}
}