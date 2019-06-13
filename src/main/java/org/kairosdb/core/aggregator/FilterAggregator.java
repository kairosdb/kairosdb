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

import org.kairosdb.core.DataPoint;
import org.kairosdb.core.annotation.FeatureComponent;
import org.kairosdb.core.annotation.FeatureProperty;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.plugin.Aggregator;


@FeatureComponent(
        name = "filter",
		description = "Filters datapoints according to filter operation with a null data point."
)
public class FilterAggregator implements Aggregator
{
	public enum FilterOperation
	{
		LTE {
			@Override
			boolean compare(double a, double b) {
				return a <= b;
			}
		},

		LT {
			@Override
			boolean compare(double a, double b) {
				return a < b;
			}
		},

		GTE {
			@Override
			boolean compare(double a, double b) {
				return a >= b;
			}
		},

		GT {
			@Override
			boolean compare(double a, double b) {
				return a > b;
			}
		},

		EQUAL {
			@Override
			boolean compare(double a, double b) {
				return a == b;
			}
		};

		abstract boolean compare(double a, double b);
	}

    public FilterAggregator()
	{
		m_threshold = 0.0;
	}

	public FilterAggregator(FilterOperation filterop, double threshold)
	{
		m_filterop = filterop;
		m_threshold = threshold;
	}

	@FeatureProperty(
			name = "filter_op",
			label = "Filter operation",
			description = "The operation performed for each data point.",
			type = "enum",
			options = {"lte", "lt", "gte", "gt", "equal"},
			default_value = "equal"
	)
	private FilterOperation m_filterop;

	@FeatureProperty(
			label = "Threshold",
			description = "The value the operation is performed on. If the operation is lt, then a null data point is returned if the data point is less than the threshold."
	)
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
				if (m_filterop.compare(x0, m_threshold))
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
