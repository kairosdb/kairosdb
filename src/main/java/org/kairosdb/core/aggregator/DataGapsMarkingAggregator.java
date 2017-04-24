/*
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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.annotation.QueryProcessor;
import org.kairosdb.core.datapoints.NullDataPoint;

import java.util.Collections;
import java.util.Iterator;

@QueryProcessor(
        name = "gaps",
		description = "Marks gaps in data according to sampling rate with a null data point."
)
public class DataGapsMarkingAggregator extends RangeAggregator
{


	@Inject
	public DataGapsMarkingAggregator()
	{
		super(true);
	}

	@Override
	protected RangeSubAggregator getSubAggregator()
	{
		return (new MarkDataGapsAggregator());
	}

	@Override
	public boolean canAggregate(String groupType)
	{
		return true;
	}

	@Override
	public String getAggregatedGroupType(String groupType)
	{
		return groupType;
	}

	private static class MarkDataGapsAggregator implements RangeSubAggregator
	{
		@Override
		public Iterable<DataPoint> getNextDataPoints(long returnTime, Iterator<DataPoint> dataPointRange)
		{
			if (dataPointRange.hasNext())
			{
				return ImmutableList.copyOf(dataPointRange);
			}

			return Collections.singletonList(new NullDataPoint(returnTime));
		}
	}
}