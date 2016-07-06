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

import com.google.inject.Inject;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.annotation.AggregatorName;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.datapoints.FractionDataPoint;
import org.kairosdb.core.datapoints.FractionDataPointFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Converts all longs to double. This will cause a loss of precision for very large long values.
 */
@AggregatorName(name = "weighted_avg", description = "Compute weighted avg from Fraction data points.")
public class WeightedAvgAggregator extends RangeAggregator
{
	public static final Logger logger = LoggerFactory.getLogger(WeightedAvgAggregator.class);

	private DoubleDataPointFactory m_dataPointFactory;

	@Inject
	public WeightedAvgAggregator(DoubleDataPointFactory dataPointFactory)
	{
		m_dataPointFactory = dataPointFactory;
	}

    	@Override
	public boolean canAggregate(String groupType)
	{
		return FractionDataPointFactory.GROUP_TYPE.equals(groupType);
	}
       	@Override
	public String getGroupType(String groupType)
	{
	        return m_dataPointFactory.getGroupType();
	}
	@Override
	protected RangeSubAggregator getSubAggregator()
	{
		return (new WeightedAvgDataPointAggregator());
	}

	private class WeightedAvgDataPointAggregator implements RangeSubAggregator
	{

		@Override
		public Iterable<DataPoint> getNextDataPoints(long returnTime, Iterator<DataPoint> dataPointRange)
		{
			double numerator = 0;
			double denominator = 0;
			int counter = 0;
			while (dataPointRange.hasNext())
			{
			    FractionDataPoint dp = (FractionDataPoint)dataPointRange.next();
				numerator += dp.getDoubleNumerator();
				denominator += dp.getDoubleDenominator();
				counter ++;
			}

			if (logger.isDebugEnabled())
			{
				logger.debug("Aggregating "+counter+" values");
			}

			return Collections.singletonList(m_dataPointFactory.createDataPoint(returnTime, numerator / denominator));
		}
	}
}
