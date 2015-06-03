package org.kairosdb.core.aggregator;

import com.google.inject.Inject;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.annotation.AggregatorName;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.datastore.DataPointGroup;

/**
 Created by bhawkins on 12/16/14.
 */
@AggregatorName(name = "diff", description = "Computes the difference between successive data points.")
public class DiffAggregator implements Aggregator
{
	private DoubleDataPointFactory m_dataPointFactory;
	private boolean m_negativeFilter;

	@Inject
	public DiffAggregator(DoubleDataPointFactory dataPointFactory)
	{
		m_dataPointFactory = dataPointFactory;
		m_negativeFilter = false;
	}

	public void setNegativeFilter(boolean filter)
	{
		m_negativeFilter = filter;
	}

	@Override
	public DataPointGroup aggregate(DataPointGroup dataPointGroup)
	{
		return new DiffDataPointGroup(dataPointGroup);
	}

	@Override
	public boolean canAggregate(String groupType)
	{
		return DataPoint.GROUP_NUMBER.equals(groupType);
	}

	private class DiffDataPointGroup extends AggregatedDataPointGroupWrapper
	{
		double m_lastValue;

		public DiffDataPointGroup(DataPointGroup innerDataPointGroup)
		{
			super(innerDataPointGroup);

			if (currentDataPoint != null)
				m_lastValue = currentDataPoint.getDoubleValue();
		}

		@Override
		public boolean hasNext()
		{
			return currentDataPoint != null && hasNextInternal();
		}

		@Override
		public DataPoint next()
		{
			//final double lastValue = currentDataPoint.getDoubleValue();

			//This defaults the rate to 0 if no more data points exists
			double newValue = m_lastValue;

			if (hasNextInternal())
			{
				currentDataPoint = nextInternal();

				newValue = currentDataPoint.getDoubleValue();
			}

			double diff = newValue - m_lastValue;

			if (m_negativeFilter && (diff < 0))
			{
				diff = 0.0;
			}
			else
				m_lastValue = newValue;


			return (m_dataPointFactory.createDataPoint(currentDataPoint.getTimestamp(), diff));
		}
	}
}
