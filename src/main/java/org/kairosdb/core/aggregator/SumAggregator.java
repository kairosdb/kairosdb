/*
 * Copyright 2016 KairosDB Authors
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
import org.kairosdb.core.annotation.QueryProcessor;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;

/**
 * Converts all longs to double. This will cause a loss of precision for very large long values.
 */
@QueryProcessor(
        name = "sum",
		description = "Adds data points together."
)
public class SumAggregator extends RangeAggregator
{
	public static final Logger logger = LoggerFactory.getLogger(SumAggregator.class);

	private DoubleDataPointFactory m_dataPointFactory;

	@Inject
	public SumAggregator(DoubleDataPointFactory dataPointFactory)
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
		return (new SumDataPointAggregator());
	}

	private class SumDataPointAggregator implements RangeSubAggregator
	{

		@Override
		public Iterable<DataPoint> getNextDataPoints(long returnTime, Iterator<DataPoint> dataPointRange)
		{
			double sum = 0;
			int counter = 0;
			while (dataPointRange.hasNext())
			{
				sum += dataPointRange.next().getDoubleValue();
				counter ++;
			}

			if (logger.isDebugEnabled())
			{
				logger.debug("Aggregating "+counter+" values");
			}

			return Collections.singletonList(m_dataPointFactory.createDataPoint(returnTime, sum));
		}
	}
}
