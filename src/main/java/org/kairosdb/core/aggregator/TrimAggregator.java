package org.kairosdb.core.aggregator;

import com.google.common.base.MoreObjects;
import com.google.inject.Inject;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.annotation.FeatureComponent;
import org.kairosdb.core.annotation.FeatureProperty;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.plugin.Aggregator;

/**
 Trims off the first, last or both (first and last) data points.  When aggregating
 the first and last data points may not represent a full range and you may want
 to remove them for displaying in a graph or in conjunction with the save_as
 aggregator to save rollup data.

 Created by bhawkins on 8/28/15.
 */
@FeatureComponent(
        name = "trim",
		description = "Trims off the first, last or both (first and last) data points from the results."
)
public class TrimAggregator implements Aggregator
{
	public enum Trim
	{
		FIRST, LAST, BOTH
	}

	@Inject
	public TrimAggregator()
	{
	}

	public TrimAggregator(Trim trim)
	{
		m_trim = trim;
	}

	@FeatureProperty(
			name = "trim",
			label = "Trim",
			description = "Which data point to trim",
			type = "enum",
			default_value = "both"
	)
	private Trim m_trim;

	@Override
	public DataPointGroup aggregate(DataPointGroup dataPointGroup)
	{
		return new TimDataPointAggregator(dataPointGroup);
	}

	/**
	 Sets which data points to trim off. Values can be FIRST, LAST or BOTH.
	 Setting to trim FIRST will trim off the oldest data point unless order is
	 descending.
	 @param trim trim
	 */
	public void setTrim(Trim trim)
	{
		m_trim = trim;
	}

	@Override
	public boolean canAggregate(String groupType)
	{
		return true;
	}

	@Override
	public boolean equals(Object o)
	{
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		TrimAggregator that = (TrimAggregator) o;

		return m_trim == that.m_trim;

	}

	@Override
	public int hashCode()
	{
		return m_trim != null ? m_trim.hashCode() : 0;
	}

	@Override
	public String toString()
	{
		return MoreObjects.toStringHelper(this)
				.add("m_trim", m_trim)
				.toString();
	}

	@Override
	public String getAggregatedGroupType(String groupType)
	{
		return groupType;
	}

	private class TimDataPointAggregator extends AggregatedDataPointGroupWrapper
	{
		public TimDataPointAggregator(DataPointGroup innerDataPointGroup)
		{
			super(innerDataPointGroup);

			if (m_trim == Trim.FIRST || m_trim == Trim.BOTH)
			{
				if (innerDataPointGroup.hasNext())
					currentDataPoint = innerDataPointGroup.next();
				else
					currentDataPoint = null;
			}
		}

		@Override
		public boolean hasNext()
		{
			if (m_trim == Trim.BOTH || m_trim == Trim.LAST)
			{
				return currentDataPoint != null && hasNextInternal();
			}
			else
				return super.hasNext();
		}

		@Override
		public DataPoint next()
		{
			DataPoint ret = currentDataPoint;
			if (hasNextInternal())
				currentDataPoint = nextInternal();
			else
				currentDataPoint = null;

			return ret;
		}


	}
}
