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

package org.kairosdb.core.aggregator;

import org.joda.time.DateTime;
import org.joda.time.Months;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.Sampling;
import org.kairosdb.core.datastore.TimeUnit;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class RangeAggregator implements Aggregator
{
	private long m_startTime = 0L;
	private long m_range = 1L;
	private long m_currentRange;
	private long m_dayOfMonthOffset = 0L; //day of month offset in milliseconds
	private boolean m_alignSampling;

	@NotNull
	@Valid
	private Sampling m_sampling;
	private boolean m_alignStartTime;

	public DataPointGroup aggregate(DataPointGroup dataPointGroup)
	{
		checkNotNull(dataPointGroup);

		if (m_alignSampling)
		{
			TimeUnit tu = m_sampling.getUnit();

			DateTime dt = new DateTime(m_startTime);
			switch (tu)
			{
				case YEARS:
				case MONTHS:
				case WEEKS:
				case DAYS:
					if (tu == TimeUnit.WEEKS)
						dt = dt.withDayOfWeek(1);
					else if (tu == TimeUnit.MONTHS)
					{
						dt = dt.withDayOfMonth(1);
						m_dayOfMonthOffset = 0;
					}
					else
						dt = dt.withDayOfYear(1);
                 
				case HOURS:
				case MINUTES:
				case SECONDS:
				case MILLISECONDS:
					dt = dt.withHourOfDay(0);
					dt = dt.withMinuteOfHour(0);
					dt = dt.withSecondOfMinute(0);
					dt = dt.withMillisOfSecond(0);
			}

			m_startTime = dt.getMillis();
		}
		m_currentRange = m_startTime;

		return (new RangeDataPointAggregator(dataPointGroup, getSubAggregator()));
	}

	public void setSampling(Sampling sampling)
	{
        m_sampling = sampling;
		m_range = sampling.getSampling();
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
		//Get the day of the month for month calculations
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		cal.setTimeInMillis(startTime);
		int dayOfMonth = cal.get(Calendar.DAY_OF_MONTH);
		dayOfMonth -= 1; //offset this value so when we subtract it from the data point tome it wont do anything for 1
		m_dayOfMonthOffset = dayOfMonth * 24L * 60L * 60L * 1000L;
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


	//===========================================================================
	/**

	 */
	private class RangeDataPointAggregator extends AggregatedDataPointGroupWrapper
	{
		private RangeSubAggregator m_subAggregator;
		private Calendar m_calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		private Iterator<DataPoint> m_dpIterator;


		public RangeDataPointAggregator(DataPointGroup innerDataPointGroup,
				RangeSubAggregator subAggregator)
		{
			super(innerDataPointGroup);
			m_subAggregator = subAggregator;
			m_dpIterator = new ArrayList<DataPoint>().iterator();
		}

		/**
		 This returns some value that represents the range the timestamp falls into
		 The actual value returned is not considered just as long as it is unique
		 for the time range.
		 @param timestamp
		 @return
		 */
		/*private long getRange(long timestamp)
		{
			while (timestamp > (m_currentRange + m_range))
				m_currentRange += m_range;

			return (m_currentRange);

			if ((m_sampling != null) && (m_sampling.getUnit() == TimeUnit.MONTHS))
			{
				m_calendar.setTimeInMillis(timestamp - m_dayOfMonthOffset);
				int dataPointYear = m_calendar.get(Calendar.YEAR);
				int dataPointMonth = m_calendar.get(Calendar.MONTH);

				return ((dataPointYear * 12 + dataPointMonth) / m_sampling.getValue());
			}
			else
			{
				return ((timestamp - m_startTime) / m_range);
			}
		}*/

		private long getStartRange(long timestamp)
		{
			if ((m_sampling != null) && (m_sampling.getUnit() == TimeUnit.MONTHS))
			{
				DateTime start = new DateTime(m_startTime);
				DateTime dpTime = new DateTime(timestamp);

				Months months = Months.monthsBetween(start, dpTime);
				Months period = months.dividedBy(m_sampling.getValue());

				long startRange = start.plus(period.multipliedBy(m_sampling.getValue())).getMillis();
				return (startRange);

				/*m_calendar.setTimeInMillis(timestamp - m_dayOfMonthOffset);
				int dataPointYear = m_calendar.get(Calendar.YEAR);
				int dataPointMonth = m_calendar.get(Calendar.MONTH);

				return ((dataPointYear * 12 + dataPointMonth) / m_sampling.getValue());*/
			}
			else
			{
				return (((timestamp - m_startTime) / m_range) * m_range + m_startTime);
			}
		}

		private long getEndRange(long timestamp)
		{
			if ((m_sampling != null) && (m_sampling.getUnit() == TimeUnit.MONTHS))
			{
				DateTime start = new DateTime(m_startTime);
				DateTime dpTime = new DateTime(timestamp);

				Months months = Months.monthsBetween(start, dpTime);
				Months period = months.dividedBy(m_sampling.getValue());

				long endRange = start.plus(period.plus(1).multipliedBy(m_sampling.getValue())).getMillis();
				return (endRange);
			}
			else
			{
				return ((((timestamp - m_startTime) / m_range) +1) * m_range + m_startTime);
			}
		}

		@Override
		public DataPoint next()
		{
			if (!m_dpIterator.hasNext())
			{
				//We calculate start and end ranges as the ranges may not be
				//consecutive if data does not show up in each range.
                // TODO fix sampling for DST
				long startRange = getStartRange(currentDataPoint.getTimestamp());
				long endRange = getEndRange(currentDataPoint.getTimestamp());

                Logger logger = LoggerFactory.getLogger(this.getClass().getName());
                logger.info((new Date(startRange)).toString());
//                logger.info((new Date(endRange)).toString());

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

		@Override
		public boolean hasNext()
		{
			return (m_dpIterator.hasNext() || super.hasNext());
		}

		//========================================================================
		/**
		 This class provides an iterator over a discrete range of data points
		 */
		private class SubRangeIterator implements Iterator<DataPoint>
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
