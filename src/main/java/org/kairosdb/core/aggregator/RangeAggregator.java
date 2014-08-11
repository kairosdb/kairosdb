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
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.Sampling;
import org.kairosdb.core.datastore.TimeUnit;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class RangeAggregator implements Aggregator
{
	private long m_startTime = 0L;
	private boolean m_alignSampling;

	@NotNull
	@Valid
	private Sampling m_sampling;
	private boolean m_alignStartTime;

	public DataPointGroup aggregate(DataPointGroup dataPointGroup)
	{
        if (m_sampling == null) // TODO is this clean?
            m_sampling = new Sampling(1, TimeUnit.MILLISECONDS);
		checkNotNull(dataPointGroup);

		if (m_alignSampling)
		{
            m_startTime = alignRangeBoundary(m_startTime);
		}

		return (new RangeDataPointAggregator(dataPointGroup, getSubAggregator()));
	}

    /**
     * For YEARS, MONTHS, WEEKS, DAYS:
     *     Computes the timestamp of the first millisecond of the day
     *     of the timestamp.
     * For HOURS,
     *     Computes the timestamp of the first millisecond of the hour
     *     of the timestamp.
     * For MINUTES,
     *     Computes the timestamp of the first millisecond of the minute
     *     of the timestamp.
     * For SECONDS,
     *     Computes the timestamp of the first millisecond of the second
     *     of the timestamp.
     * For MILLISECONDS,
     *     returns the timestamp
     * @param timestamp
     * @return
     */
    private long alignRangeBoundary(long timestamp) {
            DateTime dt = new DateTime(timestamp, m_sampling.getTimeZone());
            TimeUnit tu = m_sampling.getUnit();
            switch (tu) {
                case YEARS:
                case MONTHS:
                case WEEKS:
                case DAYS:
                    dt = dt.withMillisOfDay(0);
                    break;
                case HOURS:
                    dt = dt.withMinuteOfHour(0);
                    dt = dt.withSecondOfMinute(0);
                    dt = dt.withMillisOfSecond(0);
                    break;
                case MINUTES:
                    dt = dt.withSecondOfMinute(0);
                    dt = dt.withMillisOfSecond(0);
                    break;
                case SECONDS:
                    dt = dt.withMillisOfSecond(0);
                    break;
                default:
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


	//===========================================================================
	/**

	 */
	private class RangeDataPointAggregator extends AggregatedDataPointGroupWrapper
	{
		private RangeSubAggregator m_subAggregator;
		private Iterator<DataPoint> m_dpIterator;
        private int m_rangeIteration;
        private DateTime m_dtStartTime;
        /* used for generic range computations */
        private DateTime.Property m_dateTimeProperty;


        public RangeDataPointAggregator(DataPointGroup innerDataPointGroup,
				RangeSubAggregator subAggregator)
		{
			super(innerDataPointGroup);
            m_rangeIteration = 0;
			m_subAggregator = subAggregator;
			m_dpIterator = new ArrayList<DataPoint>().iterator();
            m_dtStartTime = new DateTime(m_startTime);

            TimeUnit tu = m_sampling.getUnit();
            switch (tu) {
                case YEARS:
                    m_dateTimeProperty = m_dtStartTime.year();
                    break;
                case MONTHS:
                    m_dateTimeProperty = m_dtStartTime.monthOfYear();
                    break;
                case WEEKS:
                    m_dateTimeProperty = m_dtStartTime.weekOfWeekyear();
                    break;
                case DAYS:
                    m_dateTimeProperty = m_dtStartTime.dayOfMonth();
                    break;
                case HOURS:
                    m_dateTimeProperty = m_dtStartTime.hourOfDay();
                    break;
                case MINUTES:
                    m_dateTimeProperty = m_dtStartTime.minuteOfHour();
                    break;
                case SECONDS:
                    m_dateTimeProperty = m_dtStartTime.secondOfMinute();
                    break;
                default:
                    m_dateTimeProperty = m_dtStartTime.millisOfSecond();
                    break;
            }
		}


		private long getStartRange()
		{
            DateTime startRange = m_dateTimeProperty.addToCopy(
                    m_sampling.getValue() * m_rangeIteration
            );
            return startRange.getMillis();
        }

		private long getEndRange()
		{
			DateTime endRange = m_dateTimeProperty.addToCopy(
                    m_sampling.getValue() * (m_rangeIteration + 1)
            );
            return endRange.getMillis();
		}

        /**
         * Iterates over the next subrange and computes the aggregated value.
         * @return a data point with the aggregated value
         */
		@Override
		public DataPoint next()
		{
			if (!m_dpIterator.hasNext())
			{
                long startRange = getStartRange();
                SubRangeIterator subIterator = null;
                do {
                    //We calculate start and end ranges as the ranges may not be
                    //consecutive if data does not show up in each range.
                    long endRange = getEndRange();

                    subIterator = new SubRangeIterator(endRange);
                    m_rangeIteration++;

                } while (!subIterator.hasNext()); // skip over empty sub ranges

				long dataPointTime = getDataPointTime();

                m_dpIterator = m_subAggregator
                        .getNextDataPoints(dataPointTime, subIterator)
                        .iterator();

			}

			return (m_dpIterator.next());
		}


        /**
         * Computes the data point time for the aggregated value.
         * Different strategies could be added here such as
         * datapoint time = range start time
         *                = range end time
         *                = range median
         *                = current datapoint time
         * @return
         */
        private long getDataPointTime() {
            return currentDataPoint.getTimestamp();
        }

        /**
         *
         * @return true if there is a subrange left
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
					currentDataPoint = nextInternal(); // set to null by hasNextInternal if no next

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
		 Returns an aggregated data point from a range that is passed in
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
