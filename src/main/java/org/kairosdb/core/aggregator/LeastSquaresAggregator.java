package org.kairosdb.core.aggregator;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.annotation.AggregatorName;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 5/29/13
 Time: 3:10 PM
 To change this template use File | Settings | File Templates.
 */
@AggregatorName(name = "least_squares", description = "Returns a best fit line through the datapoints using the least squares algorithm.")
public class LeastSquaresAggregator extends RangeAggregator
{
	@Override
	protected RangeSubAggregator getSubAggregator()
	{
		return new LeastSquaresDataPointAggregator();
	}

	private class LeastSquaresDataPointAggregator implements RangeSubAggregator
	{
		private SimpleRegression m_simpleRegression;

		public LeastSquaresDataPointAggregator()
		{
			m_simpleRegression = new SimpleRegression(true);
		}

		@Override
		public Iterable<DataPoint> getNextDataPoints(long returnTime, Iterator<DataPoint> dataPointRange)
		{
			long start = -1L;
			long stop = -1L;

			while (dataPointRange.hasNext())
			{
				DataPoint dp = dataPointRange.next();

				stop = dp.getTimestamp();
				if (start == -1L)
					start = dp.getTimestamp();

				m_simpleRegression.addData(dp.getTimestamp(), dp.getDoubleValue());
			}

			List<DataPoint> ret = new ArrayList<DataPoint>();
			ret.add(new DataPoint(start, m_simpleRegression.predict(start)));
			ret.add(new DataPoint(stop, m_simpleRegression.predict(stop)));

			return (ret);
		}
	}
}
