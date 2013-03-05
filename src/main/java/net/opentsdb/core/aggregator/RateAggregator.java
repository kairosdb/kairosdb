package net.opentsdb.core.aggregator;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.datastore.DataPointGroup;

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
			final long x0 = currentDataPoint.getTimestamp();
			final double y0 = currentDataPoint.getDoubleValue();

			//This defaults the rate to 0 if no more data points exists
			long x1 = x0+1;
			double y1 = y0;

			if (hasNextInternal())
			{
				currentDataPoint = nextInternal();

				x1 = currentDataPoint.getTimestamp();
				y1 = currentDataPoint.getDoubleValue();
			}

			double rate = (x1 - x0)/(y1 - y0);

			return (new DataPoint(x0, rate));
		}
	}
}
