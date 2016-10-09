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
import com.google.inject.name.Named;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.annotation.AggregatorName;
import org.kairosdb.core.datapoints.DataPointFactory;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.exception.KairosDBException;

import java.util.Collections;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.kairosdb.core.DataPoint.API_DOUBLE;

/**
 * Converts all longs to double. This will cause a loss of precision for very large long values.
 */
@AggregatorName(name = "avg", description = "Averages the data points together.")
public class AvgAggregator extends RangeAggregator
{
	DoubleDataPointFactory m_dataPointFactory;

	@Inject
	public AvgAggregator(DoubleDataPointFactory dataPointFactory) throws KairosDBException
	{
		m_dataPointFactory = dataPointFactory;
	}

	@Override
	protected RangeSubAggregator getSubAggregator()
	{
		return (new AvgDataPointAggregator());
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

	private class AvgDataPointAggregator implements RangeSubAggregator
	{

		@Override
		public Iterable<DataPoint> getNextDataPoints(long returnTime, Iterator<DataPoint> dataPointRange)
		{
			int count = 0;
			double sum = 0;
			while (dataPointRange.hasNext())
			{
				DataPoint dp = dataPointRange.next();
				if (dp.isDouble())
				{
					sum += dp.getDoubleValue();
					count++;
				}
			}

			return Collections.singletonList(m_dataPointFactory.createDataPoint(returnTime, sum / count));
		}
	}

}