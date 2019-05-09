package org.kairosdb.rollup;

import org.kairosdb.core.aggregator.Sampling;
import org.kairosdb.core.datastore.Duration;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RollupUtil
{
	private RollupUtil(){}

	public static List<SamplingPeriod> getSamplingPeriods(Sampling sampling, long startTime, long endTime)
	{
		List<SamplingPeriod> samplingPeriods = new ArrayList<>();
		if (startTime == endTime)
		{
			return Collections.emptyList();
		}

		long nextStartTime = startTime;
		while(nextStartTime < endTime)
		{
			long periodStart = nextStartTime;
			nextStartTime = getNextStartTime(sampling, nextStartTime);
			samplingPeriods.add(new SamplingPeriod(periodStart, nextStartTime));
		}
		return samplingPeriods;
	}

	/**
	 Returns a list of times for the specified time range aligned to a sampling boundary. For example, if the sampling
	 is an hour, this method returns all the times between the start and end time that fall on the hour boundary.
	 @param sampling sampling
	 @param startTime starting time
	 @param endTime ending time
	 @return list of times for the specified time range aligned to the sampling
	 */
	public static List<SamplingPeriod> getSamplingPeriodsAlignedToUnit(Sampling sampling, long startTime, long endTime)
	{
		List<SamplingPeriod> samplingPeriods = new ArrayList<>();
		long startTimeAligned = alignToSamplingBoundary(sampling, startTime);
		long endTimeAligned = alignToSamplingBoundary(sampling, endTime);

		if (startTimeAligned == endTimeAligned)
		{
			return Collections.emptyList();
		}

		long nextStartTime = startTimeAligned;
		while( nextStartTime < endTimeAligned)
		{
			long periodStart = nextStartTime;
			nextStartTime = getNextStartTime(sampling, nextStartTime);
			samplingPeriods.add(new SamplingPeriod(periodStart, nextStartTime));
		}
		return samplingPeriods;
	}

	private static ChronoUnit convertSamplingToChronoUnit(Duration duration)
	{
		switch(duration.getUnit())
		{
			case YEARS:
				return ChronoUnit.YEARS;
			case MONTHS:
				return ChronoUnit.MONTHS;
			case DAYS:
				return ChronoUnit.DAYS;
			case HOURS:
				return ChronoUnit.HOURS;
			case MINUTES:
				return ChronoUnit.MINUTES;
			case SECONDS:
				return ChronoUnit.SECONDS;
			case MILLISECONDS:
				return ChronoUnit.MILLIS;
			default:
				throw new IllegalArgumentException("Cannot convert sampling time to ChronoUnit for " + duration.getUnit().toString());
		}
	}

	private static LocalDateTime trucateTo(LocalDateTime time, Sampling sampling)
	{
		ChronoUnit chronoUnit = convertSamplingToChronoUnit(sampling);
		// NOTE: LocalDateTime.truncatedTo(Month | Year) will throw an exception. Need to special case it
		if (chronoUnit == ChronoUnit.MONTHS)
		{
			return time.truncatedTo(ChronoUnit.DAYS).withDayOfMonth(1);
		}
		else if (chronoUnit == ChronoUnit.YEARS)
		{
			return time.truncatedTo(ChronoUnit.DAYS).withDayOfMonth(1).withMonth(1);
		}
		else
		{
			return time.truncatedTo(chronoUnit);
		}
	}

	private static long alignToSamplingBoundary(Sampling sampling, long timestamp)
	{
		LocalDateTime localDateTime =	LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC);
		localDateTime = trucateTo(localDateTime, sampling);
		return localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
	}

	private static long getNextStartTime(Sampling sampling, long currentStartTime)
	{
		ChronoUnit chronoUnit = convertSamplingToChronoUnit(sampling);
		LocalDateTime localDateTime =	LocalDateTime.ofInstant(Instant.ofEpochMilli(currentStartTime), ZoneOffset.UTC);
		return localDateTime.plus(sampling.getValue(), chronoUnit).toInstant(ZoneOffset.UTC).toEpochMilli();
	}

	/**
	 Returns the specified time minus the duration.
	 @param time time
	 @param duration amount to subtract from the time
	 @return time minus duration
	 */
	public static long subtract(long time, Duration duration)
	{
		ChronoUnit chronoUnit = convertSamplingToChronoUnit(duration);
		LocalDateTime localDateTime =	LocalDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneOffset.UTC);
		return localDateTime.minus(duration.getValue(), chronoUnit).toInstant(ZoneOffset.UTC).toEpochMilli();
	}
}
