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

import com.google.common.collect.*;
import net.opentsdb.core.DataPoint;

import java.util.*;

import static net.opentsdb.util.Preconditions.checkNotNullOrEmpty;

public abstract class AbstractDataPointGroup implements DataPointGroup
{
	private String name;
	private TreeMultimap<String, String> tags = TreeMultimap.create();

	public AbstractDataPointGroup(String name)
	{
		this.name = name;
	}

	public AbstractDataPointGroup(String name, SetMultimap<String, String> tags)
	{
		this.name = checkNotNullOrEmpty(name);
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