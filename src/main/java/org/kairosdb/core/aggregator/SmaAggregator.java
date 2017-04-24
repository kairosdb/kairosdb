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
import org.kairosdb.core.annotation.QueryProcessor;
import org.kairosdb.core.annotation.QueryProperty;
import org.kairosdb.core.annotation.ValidationProperty;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.groupby.GroupByResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;

@QueryProcessor(
        name = "sma",
		label = "SMA",
		description = "Simple moving average."
)
public class SmaAggregator implements Aggregator
{
	private DoubleDataPointFactory m_dataPointFactory;

	//@NonZero
	@QueryProperty(
			label = "Size",
			description = "The period of the moving average. This is the number of data point to use each time the average is calculated.",
			default_value = "10",
            validations = {
					@ValidationProperty(
							expression = "value > 0",
							message = "Size must be greater than 0."
					)
			}
	)
	private int m_size;

	@Inject
	public SmaAggregator(DoubleDataPointFactory dataPointFactory)
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
		checkState(m_size != 0);
		return new SmaDataPointGroup(dataPointGroup);
	}

	public void setSize(int size)
	{
		m_size = size;
	}

	private class SmaDataPointGroup implements DataPointGroup
	{
		private DataPointGroup m_innerDataPointGroup;
		ArrayList<DataPoint> subSet = new ArrayList<DataPoint>();

		public SmaDataPointGroup(DataPointGroup innerDataPointGroup)
		{
			m_innerDataPointGroup = innerDataPointGroup;

			for(int i=0;i<m_size-1;i++){
				if (innerDataPointGroup.hasNext()){
					subSet.add(innerDataPointGroup.next());
				}
			}
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

			subSet.add(dp);
			if(subSet.size()>m_size){
				subSet.remove(0);
			}
			
			double sum = 0;
			for(int i=0;i<subSet.size();i++){
				DataPoint dpt = subSet.get(i);
				sum += dpt.getDoubleValue();
			}
			
			dp = m_dataPointFactory.createDataPoint(dp.getTimestamp(), sum / subSet.size());

			//System.out.println(new SimpleDateFormat("MM/dd/yyyy HH:mm").format(dp.getTimestamp())+" "+sum+" "+subSet.size());
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
