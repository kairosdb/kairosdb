package org.kairosdb.rollup;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;
import org.kairosdb.core.aggregator.Sampling;
import org.kairosdb.core.datastore.Duration;

public class RollupUtil
{
	private RollupUtil(){}

	private static TimeZone ZONE_UTC = TimeZone.getTimeZone("UTC");

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


	public static List<SamplingPeriod> getSamplingPeriodsAlignedToUnit(Sampling sampling, long startTime, long endTime)
	{
		return getSamplingPeriodsAlignedToUnit(sampling, startTime, endTime, ZONE_UTC);
	}

	/**
	 Returns a list of times for the specified time range aligned to a sampling boundary. For example, if the sampling
	 is an hour, this method returns all the times between the start and end time that fall on the hour boundary.
	 @param sampling sampling
	 @param startTime starting time
	 @param endTime ending time
	 @param timeZone the timezone in which the alignment should be performed
	 @return list of times for the specified time range aligned to the sampling
	 */
	public static List<SamplingPeriod> getSamplingPeriodsAlignedToUnit(Sampling sampling, long startTime, long endTime, TimeZone timeZone)
	{
		List<SamplingPeriod> samplingPeriods = new ArrayList<>();
		long startTimeAligned = alignToSamplingBoundary(sampling, startTime, timeZone);
		long endTimeAligned = alignToSamplingBoundary(sampling, endTime, timeZone);

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
			case WEEKS:
				return ChronoUnit.WEEKS;
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
		return alignToSamplingBoundary(sampling, timestamp, ZONE_UTC);
	}

	private static long alignToSamplingBoundary(Sampling sampling, long timestamp, TimeZone timeZone)
	{
		LocalDateTime localDateTime =	LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), timeZone.toZoneId());
		localDateTime = trucateTo(localDateTime, sampling);
		return localDateTime.atZone(timeZone.toZoneId()).toInstant().toEpochMilli();
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

	/**
	 * If the sample size is multiple units, align them on the boundary of the next highest granularity;
	 * eg. 10 min sample size should start on minute zero of hour
	 *
	 * TODO Support WEEKS, eg bi-weekly rollup
	 * - WEEKS are not supported because a week can start on a monday or a sunday depending on domain / locale.
	 * - figure out correct locale to use to find the first day of week
	 * - Example at https://stackoverflow.com/questions/28450720/get-date-of-first-day-of-week-based-on-localdate-now-in-java-8
	 */
	public static long getTimeAlignedToIntuitiveTemporalBoundary(long originalStartTime, Sampling samplingSize, TimeZone timeZone) {
		// no need to align an individual sample size as it will be aligned to the next highest granularity by definition.
		if (samplingSize.getValue() == 1) {
			return originalStartTime;
		}

		LocalDateTime originalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(originalStartTime), timeZone.toZoneId());
		LocalDateTime startTime = null;
		ChronoUnit sampleUnit = convertSamplingToChronoUnit(samplingSize);

		if (ChronoUnit.MONTHS.equals(sampleUnit)) {
			// Because months have variable lengths,
			// handle months as a special case as an offset from the beginning of the year. Eg 3 months = quarterly alignment.
			startTime = originalDateTime.withDayOfYear(1).truncatedTo(ChronoUnit.DAYS);
			while (!startTime.plusMonths(samplingSize.getValue()).isAfter(originalDateTime)) {
				startTime = startTime.plusMonths(samplingSize.getValue());
			}
			return startTime.atZone(timeZone.toZoneId()).toInstant().toEpochMilli();
		}

		switch(sampleUnit) {
			case MILLIS:
				startTime = originalDateTime.truncatedTo(ChronoUnit.SECONDS);
				break;
			case SECONDS:
				startTime = originalDateTime.truncatedTo(ChronoUnit.MINUTES);
				break;
			case MINUTES:
				startTime = originalDateTime.truncatedTo(ChronoUnit.HOURS);
				break;
			case HOURS:
				startTime = originalDateTime.truncatedTo(ChronoUnit.DAYS);
				break;
			case DAYS:
				startTime = originalDateTime.withDayOfYear(1).truncatedTo(ChronoUnit.DAYS);
				break;
		}
		if (startTime == null) {
			// unsupported ChronoUnit, return un-clamped start time
			return originalStartTime;
		}
		// brings truncated start time back as close to the original start time as possible in multiples of the samplingSize
		startTime = startTime.plus(startTime.until(originalDateTime, sampleUnit) / samplingSize.getValue() * samplingSize.getValue(), sampleUnit);
		return startTime.atZone(timeZone.toZoneId()).toInstant().toEpochMilli();
	}
}
