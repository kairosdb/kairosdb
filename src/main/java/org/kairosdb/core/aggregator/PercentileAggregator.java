/*
 * Copyright 2013 Proofpoint Inc.
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
import org.kairosdb.core.aggregator.annotation.AggregatorName;
import org.kairosdb.core.http.rest.validation.NonZero;
import org.kairosdb.util.Reservoir;
import org.kairosdb.util.UniformReservoir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import static java.lang.Math.floor;

@AggregatorName(name = "percentile", description = "finds percentile of the data range")
public class PercentileAggregator extends RangeAggregator
{
	public static final Logger logger = LoggerFactory.getLogger(PercentileAggregator.class);

	@NonZero
	private double percentile;

	public void setPercentile(double percentile)
	{
		this.percentile = percentile;
	}


	@Override
	protected RangeSubAggregator getSubAggregator()
	{
		return (new PercentileDataPointAggregator());
	}

	private class PercentileDataPointAggregator implements RangeSubAggregator
	{
		private double[] values;
		private Reservoir reservoir;
		private double percentileValue;


		@Override
		public Iterable<DataPoint> getNextDataPoints(long returnTime, Iterator<DataPoint> dataPointRange)
		{
			reservoir = new UniformReservoir();

			while (dataPointRange.hasNext())
			{
				reservoir.update(dataPointRange.next().getDoubleValue());
			}
			getAndSortValues(reservoir.getValues());
			percentileValue = getValue(percentile);

			if (logger.isDebugEnabled())
			{
				logger.debug("Aggregating the " + percentile + " percentile");
			}

			return Collections.singletonList(new DataPoint(returnTime, percentileValue));
		}

		private void getAndSortValues(double[] values){
			this.values = values;
			Arrays.sort(this.values);
		}

		/**
		 * Returns the value at the given quantile.
		 *
		 * @param quantile    a given quantile, in {@code [0..1]}
		 * @return the value in the distribution at {@code quantile}
		 */
		private double getValue(double quantile) {
			if (quantile < 0.0 || quantile > 1.0) {
				throw new IllegalArgumentException(quantile + " is not in [0..1]");
			}

			if (values.length == 0) {
				return 0.0;
			}

			final double pos = quantile * (values.length + 1);

			if (pos < 1) {
				return values[0];
			}

			if (pos >= values.length) {
				return values[values.length - 1];
			}

			final double lower = values[(int) pos - 1];
			final double upper = values[(int) pos];
			return lower + (pos - floor(pos)) * (upper - lower);
		}
	}
}
