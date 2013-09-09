package org.kairosdb.core.aggregator;

import org.kairosdb.anomalyDetection.AnalyzerDataPoint;
import org.kairosdb.anomalyDetection.AnalyzerTreeMap;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.annotation.AggregatorName;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.Sampling;

import java.util.Date;


/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 9/7/13
 Time: 7:23 AM
 To change this template use File | Settings | File Templates.
 */
@AggregatorName(name = "med", description = "Divides each data point by a divisor.")
public class MedianAggregator implements Aggregator
{
	private static final int TREE_SIZE = 150;

	@Override
	public DataPointGroup aggregate(DataPointGroup dataPointGroup)
	{
		return new MedianDataPointGroup(dataPointGroup);
	}

	public void setSampling(Sampling sampling)
	{
	}


	private class MedianDataPointGroup extends AggregatedDataPointGroupWrapper
	{
		private AnalyzerTreeMap<AnalyzerDataPoint, String> m_tree;
		private double m_lastValue = 0;

		public MedianDataPointGroup(DataPointGroup innerDataPointGroup)
		{
			super(innerDataPointGroup);

			m_tree = new AnalyzerTreeMap<AnalyzerDataPoint, String>(TREE_SIZE);
		}

		@Override
		public DataPoint next()
		{
			DataPoint dp = nextInternal();
			m_tree.put(new AnalyzerDataPoint(dp.getTimestamp(), dp.getDoubleValue()), null);

			double newValue = m_tree.lastKey().getValue() - m_tree.firstKey().getValue();
			if (m_tree.size() == TREE_SIZE)
			{
				if ((newValue > m_lastValue) && ((newValue - m_lastValue) > (m_lastValue * 0.30)))
				{
					System.out.println("Anomaly found at "+ new Date(dp.getTimestamp()) + " " + ((newValue - m_lastValue) / m_lastValue));
				}
			}

			//AnalyzerDataPoint med = m_tree.getRootKey();
			//AnalyzerDataPoint med = m_tree.firstKey();

			m_lastValue = newValue;
			return (new DataPoint(dp.getTimestamp(), newValue));
		}

		@Override
		public boolean hasNext()
		{
			return (hasNextInternal());
		}
	}
}
