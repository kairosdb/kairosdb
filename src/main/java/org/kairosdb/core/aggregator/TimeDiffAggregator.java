package org.kairosdb.core.aggregator;

import com.google.inject.Inject;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.annotation.FeatureComponent;
import org.kairosdb.core.annotation.FeatureProperty;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.TimeUnit;
import org.kairosdb.plugin.Aggregator;

@FeatureComponent(
		name = "time_diff",
		description = "Computes the time difference between successive data points."
)
public class TimeDiffAggregator implements Aggregator
{
	private final DoubleDataPointFactory m_dataPointFactory;

	@FeatureProperty(
			name = "time_unit",
			label = "Time Unit",
			description = "Unit to report the time difference as.",
			default_value = "seconds"
	)
	private TimeUnit m_timeUnit = TimeUnit.SECONDS;

	private long m_divisor;

	@Inject
	public TimeDiffAggregator(DoubleDataPointFactory dataPointFactory)
	{
		m_dataPointFactory = dataPointFactory;
	}

	public void setTimeUnit(TimeUnit timeUnit)
	{
		m_timeUnit = timeUnit;
	}

	@Override
	public DataPointGroup aggregate(DataPointGroup dataPointGroup)
	{
		return new TimeDiffDataPointGroup(dataPointGroup);
	}

	@Override
	public boolean canAggregate(String groupType)
	{
		return true;
	}

	@Override
	public String getAggregatedGroupType(String groupType)
	{
		return m_dataPointFactory.getGroupType();
	}

	@Override
	public void init()
	{
		m_divisor = 1;
		switch(m_timeUnit)
		{
			case MONTHS:
				throw new AssertionError();
			case YEARS: m_divisor *= 52;
			case WEEKS: m_divisor *= 7;
			case DAYS: m_divisor *= 24;
			case HOURS: m_divisor *= 60;
			case MINUTES: m_divisor *= 60;
			case SECONDS: m_divisor *= 1000;
		}
	}

	private class TimeDiffDataPointGroup extends AggregatedDataPointGroupWrapper
	{

		TimeDiffDataPointGroup(DataPointGroup innerDataPointGroup)
		{
			super(innerDataPointGroup);
		}

		@Override
		public boolean hasNext()
		{
			return currentDataPoint != null && hasNextInternal();
		}

		@Override
		public DataPoint next()
		{
			final long lastTime = currentDataPoint.getTimestamp();

			//This defaults the rate to 0 if no more data points exists
			long newTime = lastTime;

			if (hasNextInternal())
			{
				currentDataPoint = nextInternal();

				newTime = currentDataPoint.getTimestamp();
			}

			double diff = ((double)(newTime - lastTime)) / m_divisor;

			return (m_dataPointFactory.createDataPoint(currentDataPoint.getTimestamp(), diff));
		}
	}
}
