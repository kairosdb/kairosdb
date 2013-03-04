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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Converts all longs to double. This will cause a loss of precision for very large long values.
 */
@AggregatorName(name = "sum", description = "Adds data points together.")
public class SumAggregator extends RangeAggregator
{
	public static final Logger logger = LoggerFactory.getLogger(SumAggregator.class);


	@Override
	protected RangeSubAggregator getSubAggregator()
	{
		return (new SumDataPointAggregator());
	}

	private class SumDataPointAggregator implements RangeSubAggregator
	{

		@Override
		public DataPoint getNextDataPoint(long returnTime, Iterator<DataPoint> dataPointRange)
		{
			double sum = 0;
			int counter = 0;
			while (dataPointRange.hasNext())
			{
				sum += dataPointRange.next().getDoubleValue();
				counter ++;
			}

			if (logger.isDebugEnabled())
			{
				logger.debug("Aggregating "+counter+" values");
			}

			return new DataPoint(returnTime, sum);
		}
	}
}
