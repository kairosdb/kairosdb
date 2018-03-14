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
package org.kairosdb.core.datastore;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import org.kairosdb.plugin.Aggregator;
import org.kairosdb.plugin.GroupBy;
import org.kairosdb.util.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class QueryMetric implements DatastoreMetricQuery
{
	private long startTime;
	private long endTime;
	private boolean endTimeSet;
	private int cacheTime;
	private String name;
	private SetMultimap<String, String> tags = HashMultimap.create();
	private List<GroupBy> groupBys = new ArrayList<GroupBy>();
	private List<Aggregator> aggregators;
	private String cacheString;
	private boolean excludeTags = false;
	private int limit;
	private Order order = Order.ASC;
	private List<QueryPlugin> plugins;

	public QueryMetric(long start_time, int cacheTime, String name)
	{
		this.aggregators = new ArrayList<Aggregator>();
		this.plugins = new ArrayList<QueryPlugin>();
		this.startTime = start_time;
		this.cacheTime = cacheTime;
		this.name = Preconditions.checkNotNullOrEmpty(name);
	}

	public QueryMetric(long start_time, long end_time, int cacheTime, String name)
	{
		this.aggregators = new ArrayList<Aggregator>();
		this.plugins = new ArrayList<QueryPlugin>();
		this.startTime = start_time;
		this.endTime = end_time;
		this.endTimeSet = true;
		this.cacheTime = cacheTime;
		this.name = Preconditions.checkNotNullOrEmpty(name);
	}

	public QueryMetric addAggregator(Aggregator aggregator)
	{
		checkNotNull(aggregator);

		this.aggregators.add(aggregator);
		return (this);
	}

	public QueryMetric setTags(SetMultimap<String, String> tags)
	{
		this.tags = tags;
		return this;
	}

	public QueryMetric setTags(Map<String, String> tags)
	{
		this.tags.clear();

		for (String s : tags.keySet())
		{
			this.tags.put(s, tags.get(s));
		}

	return this;
	}

	public QueryMetric addTag(String name, String value)
	{
		this.tags.put(name, value);
		return this;
	}

	@Override
	public String getName()
	{
		return name;
	}

	public List<Aggregator> getAggregators()
	{
		return aggregators;
	}

	@Override
	public SetMultimap<String, String> getTags()
	{
		return (tags);
	}

	@Override
	public long getStartTime()
	{
		return startTime;
	}

	@Override
	public long getEndTime()
	{
		if (!endTimeSet)
			endTime = Long.MAX_VALUE;

		return endTime;
	}

	public int getCacheTime()
	{
		return cacheTime;
	}

	public void setEndTime(long endTime)
	{
		this.endTime = endTime;
		this.endTimeSet = true;
	}

	public void setStartTime(long startTime)
	{
		this.startTime = startTime;
	}

	public List<GroupBy> getGroupBys()
	{
		return Collections.unmodifiableList(groupBys);
	}

	public void addGroupBy(GroupBy groupBy)
	{
		this.groupBys.add(groupBy);
	}

	public void setCacheString(String cacheString)
	{
		this.cacheString = cacheString;
	}

	public String getCacheString()
	{
		return (cacheString);
	}

	public boolean isExcludeTags()
	{
		return excludeTags;
	}

	public void setExcludeTags(boolean excludeTags)
	{
		this.excludeTags = excludeTags;
	}

	public void setLimit(int limit)
	{
		this.limit = limit;
	}

	public int getLimit()
	{
		return (limit);
	}

	public void setOrder(Order order)
	{
		this.order = order;
	}

	public Order getOrder()
	{
		return (order);
	}

	@Override
	public List<QueryPlugin> getPlugins()
	{
		return Collections.unmodifiableList(plugins);
	}

	public void addPlugin(QueryPlugin plugin)
	{
		this.plugins.add(plugin);
	}

	@Override
	public String toString()
	{
		return "QueryMetric{" +
				"startTime=" + startTime +
				", endTime=" + endTime +
				", endTimeSet=" + endTimeSet +
				", cacheTime=" + cacheTime +
				", name='" + name + '\'' +
				", tags=" + tags +
				", groupBys=" + groupBys +
				", aggregators=" + aggregators +
				", cacheString='" + cacheString + '\'' +
				", excludeTags=" + excludeTags +
				", limit=" + limit +
				", order=" + order +
				", plugins=" + plugins +
				'}';
	}
}