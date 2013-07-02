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

import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.annotation.AggregatorName;

import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Converts all longs to double. This will cause a loss of precision for very large long values.
 */
@AggregatorName(name = "max", description = "Returns the maximum value data point for the time range.")
public class MaxAggregator extends RangeAggregator
{

	@Override
	protected RangeSubAggregator getSubAggregator()
	{
		return (new MaxDataPointAggregator());
	}

	private class MaxDataPointAggregator implements RangeSubAggregator
	{
		@Override
		public DataPoint getNextDataPoint(long returnTime, Iterator<DataPoint> dataPointRange)
		{
			double max = -Double.MAX_VALUE;
			while (dataPointRange.hasNext())
			{
				max = Math.max(max, dataPointRange.next().getDoubleValue());
			}

			return new DataPoint(returnTime, max);
		}
	}
}