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
import org.kairosdb.core.datastore.DataPointGroup;

@AggregatorName(name = "rate", description = "Computes the rate of change for the data points.")
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
