/*
 * Copyright 2016 KairosDB Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kairosdb.core.aggregator;


import com.google.inject.Inject;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.annotation.AggregatorName;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.groupby.GroupByResult;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

@AggregatorName(name = "scale", description = "Scales each data point by a factor.")
public class ScaleAggregator implements Aggregator
{
	private double m_factor;
	private DoubleDataPointFactory m_dataPointFactory;

	@Inject
	public ScaleAggregator(DoubleDataPointFactory dataPointFactory)
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
	public DataPointGroup aggregate(DataPointGroup dataPointGroup)
	{
		checkNotNull(dataPointGroup);

		return new ScaleDataPointGroup(dataPointGroup);
	}

	public void setFactor(double factor)
	{
		m_factor = factor;
	}

	private class ScaleDataPointGroup implements DataPointGroup
	{
		private DataPointGroup m_innerDataPointGroup;

		public ScaleDataPointGroup(DataPointGroup innerDataPointGroup)
		{
			m_innerDataPointGroup = innerDataPointGroup;
		}

		@Override
		public boolean hasNext()
		{
			return (m_innerDataPointGroup.hasNext());
		}

		@Override
		public DataPoint next()
		{
			DataPoint dp = m_innerDataPointGroup.next();

			dp = m_dataPointFactory.createDataPoint(dp.getTimestamp(), dp.getDoubleValue() * m_factor);

			return (dp);
		}

		@Override
		public void remove()
		{
			m_innerDataPointGroup.remove();
		}

		@Override
		public String getName()
		{
			return (m_innerDataPointGroup.getName());
		}

		@Override
		public List<GroupByResult> getGroupByResult()
		{
			return (m_innerDataPointGroup.getGroupByResult());
		}

		@Override
		public void close()
		{
			m_innerDataPointGroup.close();
		}

		@Override
		public Set<String> getTagNames()
		{
			return (m_innerDataPointGroup.getTagNames());
		}

		@Override
		public Set<String> getTagValues(String tag)
		{
			return (m_innerDataPointGroup.getTagValues(tag));
		}
	}
}