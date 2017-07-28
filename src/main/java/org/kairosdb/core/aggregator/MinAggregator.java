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

import java.util.Collections;
import java.util.Iterator;

/**
 * Converts all longs to double. This will cause a loss of precision for very large long values.
 */
@QueryProcessor(
        name = "min",
		description = "Returns the minimum value data point for the time range."
)
public class MinAggregator extends RangeAggregator
{
	private DoubleDataPointFactory m_dataPointFactory;

	@Inject
	public MinAggregator(DoubleDataPointFactory dataPointFactory)
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
		return (new MinDataPointAggregator());
	}

	private class MinDataPointAggregator implements RangeSubAggregator
	{

		@Override
		public Iterable<DataPoint> getNextDataPoints(long returnTime, Iterator<DataPoint> dataPointRange)
		{
			double min = Double.MAX_VALUE;
			while (dataPointRange.hasNext())
			{
				DataPoint dp = dataPointRange.next();

				min = Math.min(min, dp.getDoubleValue());
			}

			return Collections.singletonList(m_dataPointFactory.createDataPoint(returnTime, min));
		}
	}
}