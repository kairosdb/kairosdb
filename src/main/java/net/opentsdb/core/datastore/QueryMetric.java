// OpenTSDB2
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
package net.opentsdb.core.datastore;

import net.opentsdb.core.aggregator.Aggregator;

import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;
import static net.opentsdb.util.Preconditions.checkNotNullOrEmpty;

public class QueryMetric implements DatastoreMetricQuery
{
	private long startTime;
	private long endTime = -1;
	private int cacheTime;
	private String name;
	private Map<String, String> tags = new HashMap<String, String>();
	private String groupBy;
	private List<Aggregator> aggregators;

	public QueryMetric(long start_time, int cacheTime, String name)
	{
		this.aggregators = new ArrayList<Aggregator>();
		this.startTime = start_time;
		this.cacheTime = cacheTime;
		this.name = checkNotNullOrEmpty(name);
	}

	public QueryMetric addAggregator(Aggregator aggregator)
	{
		checkNotNull(aggregator);

		this.aggregators.add(aggregator);
		return (this);
	}

	public QueryMetric setTags(Map<String, String> tags)
	{
		this.tags = new HashMap<String, String>(tags);
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
	public SortedMap<String, String> getTags()
	{
		return new TreeMap<String, String>(tags);
	}

	@Override
	public long getStartTime()
	{
		return startTime;
	}

	@Override
	public long getEndTime()
	{
		if (endTime > -1)
			return endTime;
		return System.currentTimeMillis();
	}

	public int getCacheTime()
	{
		return cacheTime;
	}

	public void setEndTime(long endTime)
	{
		this.endTime = endTime;
	}

	public String getGroupBy()
	{
		return groupBy;
	}

	public void setGroupBy(String groupBy)
	{
		this.groupBy = groupBy;
	}
}