// OpenTSDB2
// Copyright (C) 2013 Proofpoint, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>
package net.opentsdb.core.aggregator;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.aggregator.annotation.AggregatorName;
import net.opentsdb.core.datastore.DataPointGroup;
import net.opentsdb.core.datastore.SortingDataPointGroup;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Converts all longs to double. This will cause a loss of precision for very large long values.
 */
@AggregatorName(name = "avg", description = "Averages the data points together.")
public class AvgAggregator extends RangeAggregator
{
	@Override
	protected RangeSubAggregator getSubAggregator()
	{
		return (new AvgDataPointAggregator());
	}

	private class AvgDataPointAggregator implements RangeSubAggregator
	{

		@Override
		public DataPoint getNextDataPoint(long returnTime, Iterator<DataPoint> dataPointRange)
		{
			int count = 0;
			double sum = 0;
			while (dataPointRange.hasNext())
			{
				sum += dataPointRange.next().getDoubleValue();
				count++;
			}

			return new DataPoint(returnTime, sum / count);
		}
	}

}