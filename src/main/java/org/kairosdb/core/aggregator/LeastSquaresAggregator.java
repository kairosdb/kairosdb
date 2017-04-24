package org.kairosdb.core.aggregator;

import com.google.inject.Inject;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.annotation.QueryProcessor;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@QueryProcessor(
        name = "least_squares",
		description = "Returns a best fit line through the datapoints using the least squares algorithm."
)
public class LeastSquaresAggregator extends RangeAggregator
{
	private DoubleDataPointFactory m_dataPointFactory;

	@Inject
	public LeastSquaresAggregator(DoubleDataPointFactory dataPointFactory)
	{
		m_dataPointFactory = dataPointFactory;
	}

	@Override
	public boolean canAggregate(String groupType)
	{
		return DataPoint.GROUP_NUMBER.equals(groupType);
	}

	@Override
	public String getAggregatedGroupType(String groupType)
	{
		return m_dataPointFactory.getGroupType();
	}

	@Override
	protected RangeSubAggregator getSubAggregator()
	{
		return new LeastSquaresDataPointAggregator();
	}

	private class LeastSquaresDataPointAggregator implements RangeSubAggregator
	{
		LeastSquaresDataPointAggregator()
		{
		}

		@Override
		public Iterable<DataPoint> getNextDataPoints(long returnTime, Iterator<DataPoint> dataPointRange)
		{
			long start = -1L;
			long stop = -1L;
			DataPoint first = null;
			DataPoint second = null;
			int count = 0;
			SimpleRegression simpleRegression = new SimpleRegression(true);

			while (dataPointRange.hasNext())
			{
				count ++;
				DataPoint dp = dataPointRange.next();
				if (second == null)
				{
					if (first == null)
						first = dp;
					else
						second = dp;
				}

				stop = dp.getTimestamp();
				if (start == -1L)
					start = dp.getTimestamp();

				simpleRegression.addData(dp.getTimestamp(), dp.getDoubleValue());
			}

			List<DataPoint> ret = new ArrayList<DataPoint>();

			if (count == 1)
			{
				ret.add(first);
			}
			else if (count == 2)
			{
				ret.add(first);
				ret.add(second);
			}
			else if (count != 0)
			{
				ret.add(m_dataPointFactory.createDataPoint(start, simpleRegression.predict(start)));
				ret.add(m_dataPointFactory.createDataPoint(stop, simpleRegression.predict(stop)));
			}

			return (ret);
		}
	}
}
