package org.kairosdb.rollup;

import org.junit.Test;
import org.kairosdb.core.aggregator.Sampling;
import org.kairosdb.core.datastore.TimeUnit;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class RollupUtilTest
{
	@Test
	public void testGetSamplingPeriods_minutes()
	{
		long start = ZonedDateTime.of(2019, 3, 2, 7, 12, 7, 2000000, ZoneOffset.UTC).toInstant().toEpochMilli();
		long end = ZonedDateTime.of(2019, 3, 2, 7, 19, 7, 2000000, ZoneOffset.UTC).toInstant().toEpochMilli();
		Sampling sampling = new Sampling(1, TimeUnit.MINUTES);
		List<SamplingPeriod> samplingPeriods = RollupUtil.getSamplingPeriods(sampling, start, end);

		assertThat(samplingPeriods.size(), equalTo(7));
		assertThat(samplingPeriods.get(0).getStartTime(), equalTo(time(2019, 3, 2, 7, 12, 7, 2)));
		assertThat(samplingPeriods.get(0).getEndTime(), equalTo(time(2019, 3, 2, 7, 13, 7, 2)));
		assertThat(samplingPeriods.get(1).getStartTime(), equalTo(time(2019, 3, 2, 7, 13, 7, 2)));
		assertThat(samplingPeriods.get(1).getEndTime(), equalTo(time(2019, 3, 2, 7, 14, 7, 2)));
		assertThat(samplingPeriods.get(2).getStartTime(), equalTo(time(2019, 3, 2, 7, 14, 7, 2)));
		assertThat(samplingPeriods.get(2).getEndTime(), equalTo(time(2019, 3, 2, 7, 15, 7, 2)));
		assertThat(samplingPeriods.get(3).getStartTime(), equalTo(time(2019, 3, 2, 7, 15, 7, 2)));
		assertThat(samplingPeriods.get(3).getEndTime(), equalTo(time(2019, 3, 2, 7, 16, 7, 2)));
		assertThat(samplingPeriods.get(4).getStartTime(), equalTo(time(2019, 3, 2, 7, 16, 7, 2)));
		assertThat(samplingPeriods.get(4).getEndTime(), equalTo(time(2019, 3, 2, 7, 17, 7, 2)));
		assertThat(samplingPeriods.get(5).getStartTime(), equalTo(time(2019, 3, 2, 7, 17, 7, 2)));
		assertThat(samplingPeriods.get(5).getEndTime(), equalTo(time(2019, 3, 2, 7, 18, 7, 2)));
		assertThat(samplingPeriods.get(6).getStartTime(), equalTo(time(2019, 3, 2, 7, 18, 7, 2)));
		assertThat(samplingPeriods.get(6).getEndTime(), equalTo(time(2019, 3, 2, 7, 19, 7, 2)));
	}
	
	@Test
	public void testGetSamplingPeriodsAlignedToUnit_minute_no_sampling_periods()
	{
		long start = ZonedDateTime.of(2019, 3, 2, 7, 5, 9, 0,ZoneOffset.UTC).toInstant().toEpochMilli();
		long end = ZonedDateTime.of(2019, 3, 2, 7, 5, 40, 0, ZoneOffset.UTC).toInstant().toEpochMilli();
		Sampling sampling = new Sampling(1, TimeUnit.MINUTES);
		List<SamplingPeriod> samplingPeriods = RollupUtil.getSamplingPeriodsAlignedToUnit(sampling, start, end);

		assertThat(samplingPeriods.size(), equalTo(0));
	}

	@Test
	public void testGetSamplingPeriodsAlignedToUnit_milliseconds()
	{
		long start = ZonedDateTime.of(2019, 3, 2, 7, 12, 7, 2000000, ZoneOffset.UTC).toInstant().toEpochMilli();
		long end = ZonedDateTime.of(2019, 3, 2, 7, 12, 7, 9000000, ZoneOffset.UTC).toInstant().toEpochMilli();
		Sampling sampling = new Sampling(1, TimeUnit.MILLISECONDS);
		List<SamplingPeriod> samplingPeriods = RollupUtil.getSamplingPeriodsAlignedToUnit(sampling, start, end);

		assertThat(samplingPeriods.size(), equalTo(7));
		assertThat(samplingPeriods.get(0).getStartTime(), equalTo(time(2019, 3, 2, 7, 12, 7, 2)));
		assertThat(samplingPeriods.get(0).getEndTime(), equalTo(time(2019, 3, 2, 7, 12, 7, 3)));
		assertThat(samplingPeriods.get(1).getStartTime(), equalTo(time(2019, 3, 2, 7, 12, 7, 3)));
		assertThat(samplingPeriods.get(1).getEndTime(), equalTo(time(2019, 3, 2, 7, 12, 7, 4)));
		assertThat(samplingPeriods.get(2).getStartTime(), equalTo(time(2019, 3, 2, 7, 12, 7, 4)));
		assertThat(samplingPeriods.get(2).getEndTime(), equalTo(time(2019, 3, 2, 7, 12, 7, 5)));
		assertThat(samplingPeriods.get(3).getStartTime(), equalTo(time(2019, 3, 2, 7, 12, 7, 5)));
		assertThat(samplingPeriods.get(3).getEndTime(), equalTo(time(2019, 3, 2, 7, 12, 7, 6)));
		assertThat(samplingPeriods.get(4).getStartTime(), equalTo(time(2019, 3, 2, 7, 12, 7, 6)));
		assertThat(samplingPeriods.get(4).getEndTime(), equalTo(time(2019, 3, 2, 7, 12, 7, 7)));
		assertThat(samplingPeriods.get(5).getStartTime(), equalTo(time(2019, 3, 2, 7, 12, 7, 7)));
		assertThat(samplingPeriods.get(5).getEndTime(), equalTo(time(2019, 3, 2, 7, 12, 7, 8)));
		assertThat(samplingPeriods.get(6).getStartTime(), equalTo(time(2019, 3, 2, 7, 12, 7, 8)));
		assertThat(samplingPeriods.get(6).getEndTime(), equalTo(time(2019, 3, 2, 7, 12, 7, 9)));
	}

	@Test
	public void testGetSamplingPeriodsAlignedToUnit_seconds()
	{
		long start = ZonedDateTime.of(2019, 3, 2, 7, 12, 7, 0, ZoneOffset.UTC).toInstant().toEpochMilli();
		long end = ZonedDateTime.of(2019, 3, 2, 7, 12, 14, 5000000, ZoneOffset.UTC).toInstant().toEpochMilli();
		Sampling sampling = new Sampling(1, TimeUnit.SECONDS);
		List<SamplingPeriod> samplingPeriods = RollupUtil.getSamplingPeriodsAlignedToUnit(sampling, start, end);

		assertThat(samplingPeriods.size(), equalTo(7));
		assertThat(samplingPeriods.get(0).getStartTime(), equalTo(time(2019, 3, 2, 7, 12, 7, 0)));
		assertThat(samplingPeriods.get(0).getEndTime(), equalTo(time(2019, 3, 2, 7, 12, 8, 0)));
		assertThat(samplingPeriods.get(1).getStartTime(), equalTo(time(2019, 3, 2, 7, 12, 8, 0)));
		assertThat(samplingPeriods.get(1).getEndTime(), equalTo(time(2019, 3, 2, 7, 12, 9, 0)));
		assertThat(samplingPeriods.get(2).getStartTime(), equalTo(time(2019, 3, 2, 7, 12, 9, 0)));
		assertThat(samplingPeriods.get(2).getEndTime(), equalTo(time(2019, 3, 2, 7, 12, 10, 0)));
		assertThat(samplingPeriods.get(3).getStartTime(), equalTo(time(2019, 3, 2, 7, 12, 10, 0)));
		assertThat(samplingPeriods.get(3).getEndTime(), equalTo(time(2019, 3, 2, 7, 12, 11, 0)));
		assertThat(samplingPeriods.get(4).getStartTime(), equalTo(time(2019, 3, 2, 7, 12, 11, 0)));
		assertThat(samplingPeriods.get(4).getEndTime(), equalTo(time(2019, 3, 2, 7, 12, 12, 0)));
		assertThat(samplingPeriods.get(5).getStartTime(), equalTo(time(2019, 3, 2, 7, 12, 12, 0)));
		assertThat(samplingPeriods.get(5).getEndTime(), equalTo(time(2019, 3, 2, 7, 12, 13, 0)));
		assertThat(samplingPeriods.get(6).getStartTime(), equalTo(time(2019, 3, 2, 7, 12, 13, 0)));
		assertThat(samplingPeriods.get(6).getEndTime(), equalTo(time(2019, 3, 2, 7, 12, 14, 0)));
	}

	@Test
	public void testGetSamplingPeriodsAlignedToUnit_minute()
	{
		long start = ZonedDateTime.of(2019, 3, 2, 7, 5, 9, 0,ZoneOffset.UTC).toInstant().toEpochMilli();
		long end = ZonedDateTime.of(2019, 3, 2, 7, 12, 40, 0, ZoneOffset.UTC).toInstant().toEpochMilli();
		Sampling sampling = new Sampling(1, TimeUnit.MINUTES);
		List<SamplingPeriod> samplingPeriods = RollupUtil.getSamplingPeriodsAlignedToUnit(sampling, start, end);

		assertThat(samplingPeriods.size(), equalTo(7));
		assertThat(samplingPeriods.get(0).getStartTime(), equalTo(time(2019, 3, 2, 7, 5, 0)));
		assertThat(samplingPeriods.get(0).getEndTime(), equalTo(time(2019, 3, 2, 7, 6, 0)));
		assertThat(samplingPeriods.get(1).getStartTime(), equalTo(time(2019, 3, 2, 7, 6, 0)));
		assertThat(samplingPeriods.get(1).getEndTime(), equalTo(time(2019, 3, 2, 7, 7, 0)));
		assertThat(samplingPeriods.get(2).getStartTime(), equalTo(time(2019, 3, 2, 7, 7, 0)));
		assertThat(samplingPeriods.get(2).getEndTime(), equalTo(time(2019, 3, 2, 7, 8, 0)));
		assertThat(samplingPeriods.get(3).getStartTime(), equalTo(time(2019, 3, 2, 7, 8, 0)));
		assertThat(samplingPeriods.get(3).getEndTime(), equalTo(time(2019, 3, 2, 7, 9, 0)));
		assertThat(samplingPeriods.get(4).getStartTime(), equalTo(time(2019, 3, 2, 7, 9, 0)));
		assertThat(samplingPeriods.get(4).getEndTime(), equalTo(time(2019, 3, 2, 7, 10, 0)));
		assertThat(samplingPeriods.get(5).getStartTime(), equalTo(time(2019, 3, 2, 7, 10, 0)));
		assertThat(samplingPeriods.get(5).getEndTime(), equalTo(time(2019, 3, 2, 7, 11, 0)));
		assertThat(samplingPeriods.get(6).getStartTime(), equalTo(time(2019, 3, 2, 7, 11, 0)));
		assertThat(samplingPeriods.get(6).getEndTime(), equalTo(time(2019, 3, 2, 7, 12, 0)));
	}

	@Test
	public void testGetSamplingPeriodsAlignedToUnit_hour()
	{
		long start = ZonedDateTime.of(2019, 3, 2, 7, 5, 0, 0, ZoneOffset.UTC).toInstant().toEpochMilli();
		long end = ZonedDateTime.of(2019, 3, 2, 14, 5, 0, 0, ZoneOffset.UTC).toInstant().toEpochMilli();
		Sampling sampling = new Sampling(1, TimeUnit.HOURS);
		List<SamplingPeriod> samplingPeriods = RollupUtil.getSamplingPeriodsAlignedToUnit(sampling, start, end);

		assertThat(samplingPeriods.size(), equalTo(7));
		assertThat(samplingPeriods.get(0).getStartTime(), equalTo(time(2019, 3, 2, 7, 0)));
		assertThat(samplingPeriods.get(0).getEndTime(), equalTo(time(2019, 3, 2, 8, 0)));
		assertThat(samplingPeriods.get(1).getStartTime(), equalTo(time(2019, 3, 2, 8, 0)));
		assertThat(samplingPeriods.get(1).getEndTime(), equalTo(time(2019, 3, 2, 9, 0)));
		assertThat(samplingPeriods.get(2).getStartTime(), equalTo(time(2019, 3, 2, 9, 0)));
		assertThat(samplingPeriods.get(2).getEndTime(), equalTo(time(2019, 3, 2, 10, 0)));
		assertThat(samplingPeriods.get(3).getStartTime(), equalTo(time(2019, 3, 2, 10, 0)));
		assertThat(samplingPeriods.get(3).getEndTime(), equalTo(time(2019, 3, 2, 11, 0)));
		assertThat(samplingPeriods.get(4).getStartTime(), equalTo(time(2019, 3, 2, 11, 0)));
		assertThat(samplingPeriods.get(4).getEndTime(), equalTo(time(2019, 3, 2, 12, 0)));
		assertThat(samplingPeriods.get(5).getStartTime(), equalTo(time(2019, 3, 2, 12, 0)));
		assertThat(samplingPeriods.get(5).getEndTime(), equalTo(time(2019, 3, 2, 13, 0)));
		assertThat(samplingPeriods.get(6).getStartTime(), equalTo(time(2019, 3, 2, 13, 0)));
		assertThat(samplingPeriods.get(6).getEndTime(), equalTo(time(2019, 3, 2, 14, 0)));
	}

	@Test
	public void testGetSamplingPeriodsAlignedToUnit_day()
	{
		long start = ZonedDateTime.of(2019, 3, 2, 7, 5, 0, 0, ZoneOffset.UTC).toInstant().toEpochMilli();
		long end = ZonedDateTime.of(2019, 3, 9, 14, 12, 0, 0, ZoneOffset.UTC).toInstant().toEpochMilli();
		Sampling sampling = new Sampling(1, TimeUnit.DAYS);
		List<SamplingPeriod> samplingPeriods = RollupUtil.getSamplingPeriodsAlignedToUnit(sampling, start, end);

		assertThat(samplingPeriods.size(), equalTo(7));
		assertThat(samplingPeriods.get(0).getStartTime(), equalTo(time(2019, 3, 2, 0)));
		assertThat(samplingPeriods.get(0).getEndTime(), equalTo(time(2019, 3, 3, 0)));
		assertThat(samplingPeriods.get(1).getStartTime(), equalTo(time(2019, 3, 3, 0)));
		assertThat(samplingPeriods.get(1).getEndTime(), equalTo(time(2019, 3, 4, 0)));
		assertThat(samplingPeriods.get(2).getStartTime(), equalTo(time(2019, 3, 4, 0)));
		assertThat(samplingPeriods.get(2).getEndTime(), equalTo(time(2019, 3, 5, 0)));
		assertThat(samplingPeriods.get(3).getStartTime(), equalTo(time(2019, 3, 5, 0)));
		assertThat(samplingPeriods.get(3).getEndTime(), equalTo(time(2019, 3, 6, 0)));
		assertThat(samplingPeriods.get(4).getStartTime(), equalTo(time(2019, 3, 6, 0)));
		assertThat(samplingPeriods.get(4).getEndTime(), equalTo(time(2019, 3, 7, 0)));
		assertThat(samplingPeriods.get(5).getStartTime(), equalTo(time(2019, 3, 7, 0)));
		assertThat(samplingPeriods.get(5).getEndTime(), equalTo(time(2019, 3, 8, 0)));
		assertThat(samplingPeriods.get(6).getStartTime(), equalTo(time(2019, 3, 8, 0)));
		assertThat(samplingPeriods.get(6).getEndTime(), equalTo(time(2019, 3, 9, 0)));
	}

	@Test
	public void testGetSamplingPeriodsAlignedToUnit_month()
	{
		long start = ZonedDateTime.of(2019, 3, 2, 7, 5, 7, 12, ZoneOffset.UTC).toInstant().toEpochMilli();
		long end = ZonedDateTime.of(2019, 10, 9, 14, 12, 9, 22, ZoneOffset.UTC).toInstant().toEpochMilli();
		Sampling sampling = new Sampling(1, TimeUnit.MONTHS);
		List<SamplingPeriod> samplingPeriods = RollupUtil.getSamplingPeriodsAlignedToUnit(sampling, start, end);

		assertThat(samplingPeriods.size(), equalTo(7));
		assertThat(samplingPeriods.get(0).getStartTime(), equalTo(time(2019, 3, 1, 0)));
		assertThat(samplingPeriods.get(0).getEndTime(), equalTo(time(2019, 4, 1, 0)));
		assertThat(samplingPeriods.get(1).getStartTime(), equalTo(time(2019, 4, 1, 0)));
		assertThat(samplingPeriods.get(1).getEndTime(), equalTo(time(2019, 5, 1, 0)));
		assertThat(samplingPeriods.get(2).getStartTime(), equalTo(time(2019, 5, 1, 0)));
		assertThat(samplingPeriods.get(2).getEndTime(), equalTo(time(2019, 6, 1, 0)));
		assertThat(samplingPeriods.get(3).getStartTime(), equalTo(time(2019, 6, 1, 0)));
		assertThat(samplingPeriods.get(3).getEndTime(), equalTo(time(2019, 7, 1, 0)));
		assertThat(samplingPeriods.get(4).getStartTime(), equalTo(time(2019, 7, 1, 0)));
		assertThat(samplingPeriods.get(4).getEndTime(), equalTo(time(2019, 8, 1, 0)));
		assertThat(samplingPeriods.get(5).getStartTime(), equalTo(time(2019, 8, 1, 0)));
		assertThat(samplingPeriods.get(5).getEndTime(), equalTo(time(2019, 9, 1, 0)));
		assertThat(samplingPeriods.get(6).getStartTime(), equalTo(time(2019, 9, 1, 0)));
		assertThat(samplingPeriods.get(6).getEndTime(), equalTo(time(2019, 10, 1, 0)));
	}

	@Test
	public void testGetSamplingPeriodsAlignedToUnit_year()
	{
		long start = ZonedDateTime.of(2003, 3, 2, 7, 5, 7, 12, ZoneOffset.UTC).toInstant().toEpochMilli();
		long end = ZonedDateTime.of(2010, 10, 9, 14, 12, 9, 22, ZoneOffset.UTC).toInstant().toEpochMilli();
		Sampling sampling = new Sampling(1, TimeUnit.YEARS);
		List<SamplingPeriod> samplingPeriods = RollupUtil.getSamplingPeriodsAlignedToUnit(sampling, start, end);

		assertThat(samplingPeriods.size(), equalTo(7));
		assertThat(samplingPeriods.get(0).getStartTime(), equalTo(time(2003, 1, 1, 0)));
		assertThat(samplingPeriods.get(0).getEndTime(), equalTo(time(2004, 1, 1, 0)));
		assertThat(samplingPeriods.get(1).getStartTime(), equalTo(time(2004, 1, 1, 0)));
		assertThat(samplingPeriods.get(1).getEndTime(), equalTo(time(2005, 1, 1, 0)));
		assertThat(samplingPeriods.get(2).getStartTime(), equalTo(time(2005, 1, 1, 0)));
		assertThat(samplingPeriods.get(2).getEndTime(), equalTo(time(2006, 1, 1, 0)));
		assertThat(samplingPeriods.get(3).getStartTime(), equalTo(time(2006, 1, 1, 0)));
		assertThat(samplingPeriods.get(3).getEndTime(), equalTo(time(2007, 1, 1, 0)));
		assertThat(samplingPeriods.get(4).getStartTime(), equalTo(time(2007, 1, 1, 0)));
		assertThat(samplingPeriods.get(4).getEndTime(), equalTo(time(2008, 1, 1, 0)));
		assertThat(samplingPeriods.get(5).getStartTime(), equalTo(time(2008, 1, 1, 0)));
		assertThat(samplingPeriods.get(5).getEndTime(), equalTo(time(2009, 1, 1, 0)));
		assertThat(samplingPeriods.get(6).getStartTime(), equalTo(time(2009, 1, 1, 0)));
		assertThat(samplingPeriods.get(6).getEndTime(), equalTo(time(2010, 1, 1, 0)));
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