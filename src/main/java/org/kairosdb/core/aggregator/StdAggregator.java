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
 * Standard Deviation aggregator.
 * Can compute without storing all of the data points in memory at the same
 * time.  This implementation is based upon a
 * <a href="http://www.johndcook.com/standard_deviation.html">paper by John
 * D. Cook</a>, which itself is based upon a method that goes back to a 1962
 * paper by B.  P. Welford and is presented in Donald Knuth's Art of
 * Computer Programming, Vol 2, page 232, 3rd edition
 *
 * Converts all longs to double. This will cause a loss of precision for very large long values.
*/
@QueryProcessor(
        name="dev",
		description = "Calculates the standard deviation of the time series."
)
public class StdAggregator extends RangeAggregator
{
	private DoubleDataPointFactory m_dataPointFactory;

	@Inject
	public StdAggregator(DoubleDataPointFactory dataPointFactory)
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
		return (new StdDataPointAggregator());
	}

	private class StdDataPointAggregator implements RangeSubAggregator
	{
		@Override
		public Iterable<DataPoint> getNextDataPoints(long returnTime, Iterator<DataPoint> dataPointRange)
		{
			int count = 0;
			double average = 0;
			double pwrSumAvg = 0;
			double stdDev = 0;

			while (dataPointRange.hasNext())
			{
				count++;
				DataPoint dp = dataPointRange.next();
				average += (dp.getDoubleValue() - average) / count;
				pwrSumAvg += (dp.getDoubleValue() * dp.getDoubleValue() - pwrSumAvg) / count;
				stdDev = Math.sqrt((pwrSumAvg * count - count * average * average) / (count - 1));
			}

			return Collections.singletonList(m_dataPointFactory.createDataPoint(returnTime, Double.isNaN(stdDev) ? 0 : stdDev));
		}
	}

}