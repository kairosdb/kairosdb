package org.kairosdb.core.aggregator;

import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.groupby.GroupByResult;

import java.util.List;
import java.util.Set;

/**
 This aggregator is to provide part of the limit functionality and not meant for
 direct use.
 */
public class LimitAggregator implements Aggregator
{
	private int m_limit;

	public LimitAggregator(int limit)
	{
		m_limit = limit;
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

	@Override
	public DataPointGroup aggregate(DataPointGroup dataPointGroup)
	{
		return new LimitDataPointGroup(dataPointGroup);
	}

	private class LimitDataPointGroup implements DataPointGroup
	{
		private DataPointGroup m_innerDataPointGroup;
		private int m_counter;

		public LimitDataPointGroup(DataPointGroup innerDataPointGroup)
		{
			m_innerDataPointGroup = innerDataPointGroup;
			m_counter = 0;
		}

		@Override
		public boolean hasNext()
		{
			if (m_counter == m_limit)
				return (false);
			else
				return (m_innerDataPointGroup.hasNext());
		}

		@Override
		public DataPoint next()
		{
			m_counter ++;

			return (m_innerDataPointGroup.next());
		}

		@Override
		public void remove()
		{
			m_innerDataPointGroup.remove();
		}

		@Override
		public String getName()
		{
			return (m_innerDataPointGroup.getName());
		}

		@Override
		public List<GroupByResult> getGroupByResult()
		{
			return (m_innerDataPointGroup.getGroupByResult());
		}

		@Override
		public void close()
		{
			m_innerDataPointGroup.close();
		}

		@Override
		public Set<String> getTagNames()
		{
			return (m_innerDataPointGroup.getTagNames());
		}

		@Override
		public Set<String> getTagValues(String tag)
		{
			return (m_innerDataPointGroup.getTagValues(tag));
		}
	}
}
