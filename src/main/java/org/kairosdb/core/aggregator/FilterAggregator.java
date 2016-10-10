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
import org.joda.time.DateTimeZone;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.annotation.AggregatorName;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.Sampling;
import org.kairosdb.core.datastore.TimeUnit;
import org.kairosdb.util.Util;


@AggregatorName(name = "filter", description = "Filters datapoints according to filter operation with a null data point.")
public class FilterAggregator implements Aggregator
{
	public enum FilterOperation
	{
		LTE, LT, GTE, GT, EQUAL
	}

	;

	public FilterAggregator()
	{
		m_threshold = 0.0;
	}

	public FilterAggregator(FilterOperation filterop, double threshold)
	{
		m_filterop = filterop;
		m_threshold = threshold;
	}

	private FilterOperation m_filterop;
	private double m_threshold;

	/**
	 Sets filter operation to apply to data points. Values can be LTE, LE, GTE, GT, or EQUAL.

	 @param filterop
	 */
	public void setFilterOp(FilterOperation filterop)
	{
		m_filterop = filterop;
	}

	public void setThreshold(double threshold)
	{
		m_threshold = threshold;
	}

	public DataPointGroup aggregate(DataPointGroup dataPointGroup)
	{
		return new FilterDataPointAggregator(dataPointGroup);
	}

	public boolean canAggregate(String groupType)
	{
		return true;
	}

	public String getAggregatedGroupType(String groupType)
	{
		return groupType;
	}

	private class FilterDataPointAggregator extends AggregatedDataPointGroupWrapper
	{
		public FilterDataPointAggregator(DataPointGroup innerDataPointGroup)
		{
			super(innerDataPointGroup);
		}

		public boolean hasNext()
		{
			boolean foundValidDp = false;
			while (!foundValidDp && currentDataPoint != null)
			{
				double x0 = currentDataPoint.getDoubleValue();
				if (m_filterop == FilterOperation.LTE && x0 <= m_threshold)
					moveCurrentDataPoint();
				else if (m_filterop == FilterOperation.LT && x0 < m_threshold)
					moveCurrentDataPoint();
				else if (m_filterop == FilterOperation.GTE && x0 >= m_threshold)
					moveCurrentDataPoint();
				else if (m_filterop == FilterOperation.GT && x0 > m_threshold)
					moveCurrentDataPoint();
				else if (m_filterop == FilterOperation.EQUAL && x0 == m_threshold)
					moveCurrentDataPoint();
				else
					foundValidDp = true;
			}

			return foundValidDp;
		}

		public DataPoint next()
		{
			DataPoint ret = currentDataPoint;
			moveCurrentDataPoint();
			return ret;
		}

		private void moveCurrentDataPoint()
		{
			if (hasNextInternal())
				currentDataPoint = nextInternal();
			else
				currentDataPoint = null;
		}
	}
}
