package org.kairosdb.core.aggregator;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.annotation.FeatureComponent;
import org.kairosdb.core.annotation.FeatureProperty;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datapoints.NullDataPoint;

import java.util.Collections;
import java.util.Iterator;

@FeatureComponent(
		name = "pad",
		description = "Pads each range that is missing data with a value (default is zero)."
)
public class PadAggregator extends RangeAggregator
{
	@FeatureProperty(
			name = "pad_value",
			label = "Pad value",
			description = "Value to pad each range with.",
			default_value = "0"
	)
	private long m_padValue = 0L;

	@Inject
	public PadAggregator()
	{
		super(true);
	}

	public void setPadValue(long value)
	{
		m_padValue = value;
	}

	@Override
	protected RangeSubAggregator getSubAggregator()
	{
		return new AddPadAggregator();
	}

	@Override
	public boolean canAggregate(String groupType)
	{
		return true;
	}

	@Override
	public String getAggregatedGroupType(String groupType)
	{
		return groupType;
	}

	private class AddPadAggregator implements RangeSubAggregator
	{
		@Override
		public Iterable<DataPoint> getNextDataPoints(long returnTime, Iterator<DataPoint> dataPointRange)
		{
			if (dataPointRange.hasNext())
			{
				return ImmutableList.copyOf(dataPointRange);
			}

			return Collections.singletonList(new LongDataPoint(returnTime, m_padValue));
		}
	}
}
