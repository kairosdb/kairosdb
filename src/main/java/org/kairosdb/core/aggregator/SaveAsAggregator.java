package org.kairosdb.core.aggregator;

import com.google.common.collect.ImmutableSortedMap;
import com.google.inject.Inject;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.annotation.AggregatorName;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.groupby.GroupByResult;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 Created by bhawkins on 8/28/15.
 */
@AggregatorName(name = "save_as", description = "Saves the results to a new metric.")
public class SaveAsAggregator implements Aggregator
{

	public static final int DEFAULT_TTL = 0;

	private Datastore m_datastore;
	private String m_metricName;
	private Map<String, String> m_tags;

	@Inject
	public SaveAsAggregator(Datastore datastore)
	{
		m_datastore = datastore;
		m_tags = new HashMap<>();
	}

	public void setMetricName(String metricName)
	{
		m_metricName = metricName;
	}

	public void setTags(Map<String, String> tags)
	{
		m_tags = tags;
	}

	public String getMetricName()
	{
		return m_metricName;
	}

	public Map<String, String> getTags()
	{
		return m_tags;
	}

	@Override
	public DataPointGroup aggregate(DataPointGroup dataPointGroup)
	{
		return new SaveAsDataPointAggregator(dataPointGroup);
	}

	@Override
	public boolean canAggregate(String groupType)
	{
		return true;
	}

	private class SaveAsDataPointAggregator implements DataPointGroup
	{
		private DataPointGroup m_innerDataPointGroup;
		private ImmutableSortedMap<String, String> m_groupTags;

		public SaveAsDataPointAggregator(DataPointGroup innerDataPointGroup)
		{
			m_innerDataPointGroup = innerDataPointGroup;
			ImmutableSortedMap.Builder<String, String> mapBuilder = ImmutableSortedMap.<String, String>naturalOrder();
			mapBuilder.putAll(m_tags);
			mapBuilder.put("saved_from", innerDataPointGroup.getName());

			for (String innerTag : innerDataPointGroup.getTagNames())
			{
				Set<String> tagValues = innerDataPointGroup.getTagValues(innerTag);
				if (tagValues.size() == 1)
					mapBuilder.put(innerTag, tagValues.iterator().next());
			}

			m_groupTags = mapBuilder.build();
		}

		@Override
		public boolean hasNext()
		{
			return m_innerDataPointGroup.hasNext();
		}

		@Override
		public DataPoint next()
		{
			DataPoint next = m_innerDataPointGroup.next();

			try
			{
				m_datastore.putDataPoint(m_metricName, m_groupTags, next, DEFAULT_TTL);
			}
			catch (DatastoreException e)
			{
				throw new RuntimeException("Failure to save data to "+m_metricName, e);
			}

			return next;
		}

		@Override
		public void remove()
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public String getName()
		{
			return m_innerDataPointGroup.getName();
		}

		@Override
		public List<GroupByResult> getGroupByResult()
		{
			return m_innerDataPointGroup.getGroupByResult();
		}

		@Override
		public void close()
		{
			m_innerDataPointGroup.close();
		}

		@Override
		public Set<String> getTagNames()
		{
			return m_innerDataPointGroup.getTagNames();
		}

		@Override
		public Set<String> getTagValues(String tag)
		{
			return m_innerDataPointGroup.getTagValues(tag);
		}
	}
}
