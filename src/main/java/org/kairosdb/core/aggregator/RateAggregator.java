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
import org.kairosdb.core.annotation.QueryProcessor;
import org.kairosdb.core.annotation.QueryCompoundProperty;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.TimeUnit;
import org.kairosdb.util.Util;

@QueryProcessor(
        name = "rate",
		description = "Computes the rate of change for the data points."
)
public class RateAggregator implements Aggregator, TimezoneAware
{
    @QueryCompoundProperty(
            label = "Sampling",
            order = {"Value", "Unit"}
    )
	private Sampling m_sampling;

	private DoubleDataPointFactory m_dataPointFactory;
	private DateTimeZone m_timeZone;

	@Inject
	public RateAggregator(DoubleDataPointFactory dataPointFactory)
	{
		m_sampling = new Sampling(1, TimeUnit.MILLISECONDS);
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

	public DataPointGroup aggregate(DataPointGroup dataPointGroup)
	{
		return (new RateDataPointAggregator(dataPointGroup));
	}

	public void setSampling(Sampling sampling)
	{
		m_sampling = sampling;
	}

	public void setUnit(TimeUnit timeUnit)
	{
		m_sampling = new Sampling(1, timeUnit);
	}

	@Override
	public void setTimeZone(DateTimeZone timeZone)
	{
		m_timeZone = timeZone;
	}


	private class RateDataPointAggregator extends AggregatedDataPointGroupWrapper
	{
		RateDataPointAggregator(DataPointGroup innerDataPointGroup)
		{
			super(innerDataPointGroup);
		}

		@Override
		public boolean hasNext()
		{
			//Ensure we have two data points to mess with
			return currentDataPoint != null && hasNextInternal();
		}

		@Override
		public DataPoint next()
		{
			final double x0 = currentDataPoint.getDoubleValue();
			final long y0 = currentDataPoint.getTimestamp();

			//This defaults the rate to 0 if no more data points exists
			double x1 = x0;
			long y1 = y0+1;

			if (hasNextInternal())
			{
				currentDataPoint = nextInternal();

				x1 = currentDataPoint.getDoubleValue();
				y1 = currentDataPoint.getTimestamp();

				if (y1 == y0)
				{
					throw new IllegalStateException(
							"The rate aggregator cannot compute rate for data points with the same time stamp.  "+
							"You must precede rate with another aggregator.");
				}
			}

			double rate = (x1 - x0) / (y1 - y0) * Util.getSamplingDuration(y0, m_sampling, m_timeZone);

			return (m_dataPointFactory.createDataPoint(y1, rate));
		}
	}
}
