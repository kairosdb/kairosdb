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
package org.kairosdb.core.datastore;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.TreeMultimap;
import org.kairosdb.core.groupby.GroupByResult;
import org.kairosdb.util.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class AbstractDataPointGroup implements DataPointGroup
{
	private String name;
	private TreeMultimap<String, String> tags = TreeMultimap.create();
	private List<GroupByResult> groupByResult = new ArrayList<GroupByResult>();

	public AbstractDataPointGroup(String name)
	{
		this.name = name;
	}

	public AbstractDataPointGroup(String name, SetMultimap<String, String> tags)
	{
		this.name = Preconditions.checkNotNullOrEmpty(name);
		this.tags = TreeMultimap.create(tags);
	}

	public void addTag(String name, String value)
	{
		tags.put(name, value);
	}

	public void addTags(SetMultimap<String, String> tags)
	{
		this.tags.putAll(tags);
	}

	public void addTags(Map<String, String> tags)
	{
		for (String key : tags.keySet())
		{
			this.tags.put(key, tags.get(key));
		}
	}

	public void addTags(DataPointGroup dpGroup)
	{
		for (String key : dpGroup.getTagNames())
		{
			for (String value : dpGroup.getTagValues(key))
			{
				this.tags.put(key, value);
			}
		}
	}

	public void addGroupByResult(GroupByResult groupByResult)
	{
		this.groupByResult.add(checkNotNull(groupByResult));
	}

	public List<GroupByResult> getGroupByResult()
	{
		return groupByResult;
	}

	public String getName()
	{
		return name;
	}

	@Override
	public Set<String> getTagNames()
	{
		return (tags.keySet());
	}

	@Override
	public Set<String> getTagValues(String tag)
	{
		return (tags.get(tag));
	}

	public SetMultimap<String, String> getTags()
	{
		return (ImmutableSetMultimap.copyOf(tags));
	}

	@Override
	public void remove()
	{
		throw new UnsupportedOperationException();
	}

	public abstract void close();
}