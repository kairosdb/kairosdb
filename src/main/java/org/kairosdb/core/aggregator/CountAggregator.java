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
import org.kairosdb.core.datapoints.LongDataPointFactory;

import java.util.Collections;
import java.util.Iterator;

@QueryProcessor(
        name = "count",
		description = "Counts the number of data points."
)
public class CountAggregator extends RangeAggregator
{
	LongDataPointFactory m_dataPointFactory;

	@Inject
	public CountAggregator(LongDataPointFactory dataPointFactory)
	{
		m_dataPointFactory = dataPointFactory;
	}

	@Override
	protected RangeSubAggregator getSubAggregator()
	{
		return (new CountDataPointAggregator());
	}

	@Override
	public boolean canAggregate(String groupType)
	{
		return true;
	}

	@Override
	public String getAggregatedGroupType(String groupType)
	{
		return m_dataPointFactory.getGroupType();
	}

	private class CountDataPointAggregator implements RangeSubAggregator
	{
		@Override
		public Iterable<DataPoint> getNextDataPoints(long returnTime, Iterator<DataPoint> dataPointRange)
		{
			long count = 0;
			while (dataPointRange.hasNext())
			{
				count++;

				dataPointRange.next();
			}

			return Collections.singletonList(m_dataPointFactory.createDataPoint(returnTime, count));
		}
	}
}