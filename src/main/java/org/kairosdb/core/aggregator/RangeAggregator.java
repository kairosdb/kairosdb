/*
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

package org.kairosdb.core.aggregator;

import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.GregorianChronology;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.Sampling;
import org.kairosdb.core.datastore.TimeUnit;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.TimeZone;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class RangeAggregator implements Aggregator, TimezoneAware
{
	private long m_startTime = 0L;
	private boolean m_started = false;
	private boolean m_alignSampling;
	private boolean m_exhaustive;
	private DateTimeZone m_timeZone = DateTimeZone.UTC;
	;

	@NotNull
	@Valid
	protected Sampling m_sampling = new Sampling(1, TimeUnit.MILLISECONDS);
	protected boolean m_alignStartTime;

	public RangeAggregator()
	{
		this(false);
	}

	public RangeAggregator(boolean exhaustive)
	{
		m_exhaustive = exhaustive;
	}

	public DataPointGroup aggregate(DataPointGroup dataPointGroup)
	{
		checkNotNull(dataPointGroup);

		if (m_alignSampling)
			m_startTime = alignRangeBoundary(m_startTime);

		if (m_exhaustive)
			return (new ExhaustiveRangeDataPointAggregator(dataPointGroup, getSubAggregator()));
		else
			return (new RangeDataPointAggregator(dataPointGroup, getSubAggregator()));
	}

	/**
	 For YEARS, MONTHS, WEEKS, DAYS:
	 Computes the timestamp of the first millisecond of the day
	 of the timestamp.
	 For HOURS,
	 Computes the timestamp of the first millisecond of the hour
	 of the timestamp.
	 For MINUTES,
	 Computes the timestamp of the first millisecond of the minute
	 of the timestamp.
	 For SECONDS,
	 Computes the timestamp of the first millisecond of the second
	 of the timestamp.
	 For MILLISECONDS,
	 returns the timestamp

	 @param timestamp
	 @return
	 */
	@SuppressWarnings("fallthrough")
	private long alignRangeBoundary(long timestamp)
	{
		DateTime dt = new DateTime(timestamp, m_timeZone);
		TimeUnit tu = m_sampling.getUnit();
		switch (tu)
		{
			case YEARS:
				dt = dt.withDayOfYear(1).withMillisOfDay(0);
				break;
			case MONTHS:
				dt = dt.withDayOfMonth(1).withMillisOfDay(0);
				break;
			case WEEKS:
				dt = dt.withDayOfWeek(1).withMillisOfDay(0);
				break;
			case DAYS:
			case HOURS:
			case MINUTES:
			case SECONDS:
				dt = dt.withHourOfDay(0);
				dt = dt.withMinuteOfHour(0);
				dt = dt.withSecondOfMinute(0);
			default:
				dt = dt.withMillisOfSecond(0);
				break;
		}
		return dt.getMillis();
	}

	public void setSampling(Sampling sampling)
	{
		m_sampling = sampling;
	}

	/**
	 When set to true the time for the aggregated data point for each range will
	 fall on the start of the range instead of being the value for the first
	 data point within that range.
	 @param align
	 */
	public void setAlignStartTime(boolean align)
	{
		m_alignStartTime = align;
	}

	/**
	 Setting this to true will cause the aggregation range to be aligned based on
	 the sampling size.  For example if your sample size is either milliseconds,
	 seconds, minutes or hours then the start of the range will always be at the top
	 of the hour.  The effect of setting this to true is that your data will
	 take the same shape when graphed as you refresh the data.
	 @param align Set to true to align the range on fixed points instead of
	              the start of the query.
	 */
	public void setAlignSampling(boolean align)
	{
		m_alignSampling = align;
	}

	/**
	 Start time to calculate the ranges from.  Typically this is the start
	 of the query
	 @param startTime
	 */
	public void setStartTime(long startTime)
	{
		m_startTime = startTime;
	}

	/**
	 Return a RangeSubAggregator that will be used to aggregate data over a
	 discrete range of data points.  This is called once per grouped data series.

	 For example, if one metric is queried and no grouping is done this method is
	 called once and the resulting object is called over and over for each range
	 within the results.

	 If the query were grouping by the host tag and host has values of 'A' and 'B'
	 this method will be called twice, once to aggregate results for 'A' and once
	 to aggregate results for 'B'.
	 @return
	 */
	protected abstract RangeSubAggregator getSubAggregator();

	/**
	 Sets the time zone to use for range calculations

	 @param timeZone
	 */
	public void setTimeZone(DateTimeZone timeZone)
	{
		m_timeZone = timeZone;
	}

	public Sampling getSampling()
	{
		return m_sampling;
	}

	//===========================================================================
	/**

	 */
	private class RangeDataPointAggregator extends AggregatedDataPointGroupWrapper
	{
		protected RangeSubAggregator m_subAggregator;
		protected Calendar m_calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		protected Iterator<DataPoint> m_dpIterator;
		/* used for generic range computations */
		private DateTimeField m_unitField;

		public RangeDataPointAggregator(DataPointGroup innerDataPointGroup,
				RangeSubAggregator subAggregator)
		{
			super(innerDataPointGroup);
			m_subAggregator = subAggregator;
			m_dpIterator = new ArrayList<DataPoint>().iterator();

			Chronology chronology = GregorianChronology.getInstance(m_timeZone);

			TimeUnit tu = m_sampling.getUnit();
			switch (tu)
			{
				case YEARS:
					m_unitField = chronology.year();
					break;
				case MONTHS:
					m_unitField = chronology.monthOfYear();
					break;
				case WEEKS:
					m_unitField = chronology.weekOfWeekyear();
					break;
				case DAYS:
					m_unitField = chronology.dayOfMonth();
					break;
				case HOURS:
					m_unitField = chronology.hourOfDay();
					break;
				case MINUTES:
					m_unitField = chronology.minuteOfHour();
					break;
				case SECONDS:
					m_unitField = chronology.secondOfDay();
					break;
				default:
					m_unitField = chronology.millisOfSecond();
					break;
			}
		}


		protected long getStartRange(long timestamp)
		{
			long samplingValue = m_sampling.getValue();
			long numberOfPastPeriods = m_unitField.getDifferenceAsLong(timestamp/*getDataPointTime()*/, m_startTime) / samplingValue;
			return m_unitField.add(m_startTime, numberOfPastPeriods * samplingValue);
		}

		protected long getEndRange(long timestamp)
		{
			long samplingValue = m_sampling.getValue();
			long numberOfPastPeriods = m_unitField.getDifferenceAsLong(timestamp/*getDataPointTime()*/, m_startTime) / samplingValue;
			return m_unitField.add(m_startTime, (numberOfPastPeriods + 1) * samplingValue);
		}

		@Override
		public DataPoint next()
		{
			if (!m_dpIterator.hasNext())
			{
				//We calculate start and end ranges as the ranges may not be
				//consecutive if data does not show up in each range.
				long startRange = getStartRange(currentDataPoint.getTimestamp());
				long endRange = getEndRange(currentDataPoint.getTimestamp());

				SubRangeIterator subIterator = new SubRangeIterator(
						endRange);

				long dataPointTime = currentDataPoint.getTimestamp();
				if (m_alignStartTime)
					dataPointTime = startRange;

				m_dpIterator = m_subAggregator.getNextDataPoints(dataPointTime,
						subIterator).iterator();
			}

			return (m_dpIterator.next());
		}


		/**
		 Computes the data point time for the aggregated value.
		 Different strategies could be added here such as
		 datapoint time = range start time
		 = range end time
		 = range median
		 = current datapoint time

		 @return
		 */
		private long getDataPointTime()
		{
			return currentDataPoint.getTimestamp();
		}

		/**
		 @return true if there is a subrange left
		 */
		@Override
		public boolean hasNext()
		{
			return (m_dpIterator.hasNext() || super.hasNext());
		}

		//========================================================================
		/**
		 This class provides an iterator over a discrete range of data points
		 */
		protected class SubRangeIterator implements Iterator<DataPoint>
		{
			private long m_endRange;

			public SubRangeIterator(long endRange)
			{
				m_endRange = endRange;
			}

			@Override
			public boolean hasNext()
			{
				return ((currentDataPoint != null) && (currentDataPoint.getTimestamp() < m_endRange));
			}

			@Override
			public DataPoint next()
			{
				DataPoint ret = currentDataPoint;
				if (hasNextInternal())
					currentDataPoint = nextInternal();

				return (ret);
			}

			@Override
			public void remove()
			{
				throw new UnsupportedOperationException();
			}
		}
	}

	//========================================================================
	private class ExhaustiveRangeDataPointAggregator extends RangeDataPointAggregator
	{
		private long m_nextExpectedRangeStartTime;

		public ExhaustiveRangeDataPointAggregator(DataPointGroup innerDataPointGroup, RangeSubAggregator subAggregator)
		{
			super(innerDataPointGroup, subAggregator);
			m_nextExpectedRangeStartTime = m_startTime;
		}

		private void setNextStartTime(long timeStamp)
		{
			m_nextExpectedRangeStartTime = timeStamp;
		}

		@Override
		public DataPoint next()
		{
			if (!m_dpIterator.hasNext())
			{
				//We calculate start and end ranges as the ranges may not be
				//consecutive if data does not show up in each range.
				long startTime = m_nextExpectedRangeStartTime;
				if (!m_started)
				{
					m_started = true;
					startTime = currentDataPoint.getTimestamp();
				}
				long startRange = getStartRange(startTime);
				long endRange = getEndRange(startTime);

				// Next expected range starts just after this end range
				setNextStartTime(endRange);
				SubRangeIterator subIterator = new SubRangeIterator(
						endRange);

				long dataPointTime = currentDataPoint.getTimestamp();
				if (m_alignStartTime || startRange < dataPointTime)
					dataPointTime = startRange;

				m_dpIterator = m_subAggregator.getNextDataPoints(dataPointTime,
						subIterator).iterator();
			}

			return (m_dpIterator.next());
		}
	}

	//===========================================================================
	/**
	 Instances of this object are created once per grouped data series.
	 */
	public interface RangeSubAggregator
	{
		/**
		 Returns an aggregated data point from a ragne that is passed in
		 as dataPointRange.
		 @return

		 @param
			returnTime Timestamp to use on return data point.  This is currently
		                   passing the timestamp of the first data point in the range.
		@param
			dataPointRange Range to aggregate over.
		 */
		public Iterable<DataPoint> getNextDataPoints(long returnTime, Iterator<DataPoint> dataPointRange);
	}
}
