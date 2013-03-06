// KairosDB2
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
package org.kairosdb.core.aggregator;

import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.annotation.AggregatorName;

import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;

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
@AggregatorName(name="dev", description = "Calculates the standard deviation of the time series.")
public class StdAggregator extends RangeAggregator
{
	@Override
	protected RangeSubAggregator getSubAggregator()
	{
		return (new StdDataPointAggregator());
	}

	private class StdDataPointAggregator implements RangeSubAggregator
	{
		@Override
		public DataPoint getNextDataPoint(long returnTime, Iterator<DataPoint> dataPointRange)
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

			return new DataPoint(returnTime, Double.isNaN(stdDev) ? 0 : stdDev);
		}
	}

}