package org.kairosdb.core.aggregator;

import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datastore.DataPointGroup;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 3/5/13
 Time: 2:13 PM
 To change this template use File | Settings | File Templates.
 */
public class RateAggregator extends SortedAggregator
{
	@Override
	protected DataPointGroup aggregate(DataPointGroup dataPointGroup)
	{
		return (new RateDataPointAggregator(dataPointGroup));
	}


	private class RateDataPointAggregator extends AggregatedDataPointGroupWrapper
	{
		public RateDataPointAggregator(DataPointGroup innerDataPointGroup)
		{
			super(innerDataPointGroup);
		}

		@Override
		public DataPoint next()
		{
			final double x0 = currentDataPoint.getDoubleValue();
			final long y0 = currentDataPoint.getTimestamp();

			//This defaults the rate to 0 if no more data points exists
			double x1 = x0;
			long y1 = y0+1;

			if (hasNextInternal())
			{
				currentDataPoint = nextInternal();

				x1 = currentDataPoint.getDoubleValue();
				y1 = currentDataPoint.getTimestamp();

				if (y1 == y0)
				{
					throw new IllegalStateException(
							"The rate aggregator cannot compute rate for data points with the same time stamp.  "+
							"You must precede rate with another aggregator.");
				}
			}

			double rate = (x1 - x0)/(y1 - y0);

			return (new DataPoint(y0, rate));
		}
	}
}
