//
// RangeAggregator.java
//
// Copyright 2013, NextPage Inc. All rights reserved.
//

package net.opentsdb.core.aggregator;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.datastore.DataPointGroup;

import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class RangeAggregator extends SortedAggregator
{
	private long m_startTime;
	private long m_range = 1L;

	@Override
	protected DataPointGroup aggregate(DataPointGroup dataPointGroup)
	{
		checkNotNull(dataPointGroup);

		return (new RangeDataPointAggregator(dataPointGroup, getSubAggregator()));
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
	 Range in which to do aggregation over.
	 @param range Range of time in milliseconds
	 */
	public void setRange(long range)
	{
		m_range = range;
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

		public RangeDataPointAggregator(DataPointGroup innerDataPointGroup,
				RangeSubAggregator subAggregator)
		{
			super(innerDataPointGroup);
			m_subAggregator = subAggregator;
		}

		private long getRange(long timestamp)
		{
			return ((int)((timestamp - m_startTime) / m_range));
		}

		@Override
		public DataPoint next()
		{
			SubRangeIterator subIterator = new SubRangeIterator(
					getRange(currentDataPoint.getTimestamp()));

			DataPoint ret = m_subAggregator.getNextDataPoint(currentDataPoint.getTimestamp(),
					subIterator);

			return (ret);
		}

		//========================================================================
		/**
		 This class provides an iterator over a discrete range of data points
		 */
		private class SubRangeIterator implements Iterator<DataPoint>
		{
			private long m_currentRange;

			public SubRangeIterator(long range)
			{
				m_currentRange = range;
			}

			@Override
			public boolean hasNext()
			{
				return ((currentDataPoint != null) && (currentDataPoint.getTimestamp() == m_currentRange));
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
		 @param returnTime Timestamp to use on return data point.  This is currently
		                   passing the timestamp of the first data point in the range.
		 @param dataPointRange Range to aggregate over.
		 @return
		 */
		public DataPoint getNextDataPoint(long returnTime, Iterator<DataPoint> dataPointRange);
	}
}
