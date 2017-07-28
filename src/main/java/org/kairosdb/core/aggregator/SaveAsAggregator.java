package org.kairosdb.core.aggregator;

import com.google.common.collect.ImmutableSortedMap;
import com.google.inject.Inject;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.annotation.QueryProcessor;
import org.kairosdb.core.annotation.QueryProperty;
import org.kairosdb.core.annotation.ValidationProperty;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.groupby.GroupBy;
import org.kairosdb.core.groupby.GroupByResult;
import org.kairosdb.core.groupby.TagGroupBy;

import java.util.*;

/**
 Created by bhawkins on 8/28/15.
 */
@QueryProcessor(
        name = "save_as",
		description = "Saves the results to a new metric."
)
public class SaveAsAggregator implements Aggregator, GroupByAware
{
	private Datastore m_datastore;
	private Map<String, String> m_tags;
	private int m_ttl = 0;
	private Set<String> m_tagsToKeep = new HashSet<>();
	private boolean m_addSavedFrom = true;

	@QueryProperty(
			name = "metric_name",
			label = "Save As",
			description = "The name of the new metric.",
			default_value = "<new name>",
            validations = {
					@ValidationProperty(
							expression = "!value && value.length > 0",
							message = "The name can't be empty."
					)
			}
	)
	private String m_metricName;


	@Inject
	public SaveAsAggregator(Datastore datastore)
	{
		m_datastore = datastore;
		m_tags = new HashMap<String, String>();
	}

	public SaveAsAggregator(Datastore datastore, String metricName)
	{
		this(datastore);
		m_metricName = metricName;
		m_tags = new HashMap<String, String>();
	}

	public void setAddSavedFrom(boolean addSavedFrom)
	{
		m_addSavedFrom = addSavedFrom;
	}

	public void setMetricName(String metricName)
	{
		m_metricName = metricName;
	}

	public void setTags(Map<String, String> tags)
	{
		m_tags = tags;
	}

	public void setTtl(int ttl)
	{
		m_ttl = ttl;
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

	@Override
	public String getAggregatedGroupType(String groupType)
	{
		return groupType;
	}

	@Override
	public void setGroupBys(List<GroupBy> groupBys)
	{
		for (GroupBy groupBy : groupBys)
		{
			if (groupBy instanceof TagGroupBy)
			{
				TagGroupBy tagGroupBy = (TagGroupBy) groupBy;

				m_tagsToKeep.addAll(tagGroupBy.getTagNames());
			}
		}
	}

	private class SaveAsDataPointAggregator implements DataPointGroup
	{
		private DataPointGroup m_innerDataPointGroup;
		private ImmutableSortedMap<String, String> m_groupTags;

		public SaveAsDataPointAggregator(DataPointGroup innerDataPointGroup)
		{
			m_innerDataPointGroup = innerDataPointGroup;
			ImmutableSortedMap.Builder<String, String> mapBuilder = ImmutableSortedMap.naturalOrder();
			mapBuilder.putAll(m_tags);
			if (m_addSavedFrom)
				mapBuilder.put("saved_from", innerDataPointGroup.getName());

			for (String innerTag : innerDataPointGroup.getTagNames())
			{
				Set<String> tagValues = innerDataPointGroup.getTagValues(innerTag);
				if (m_tagsToKeep.contains(innerTag) && (tagValues.size() == 1))
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
				m_datastore.putDataPoint(m_metricName, m_groupTags, next, m_ttl);
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
