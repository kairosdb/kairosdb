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

package org.kairosdb.core;

import com.google.common.collect.ImmutableSortedMap;
import org.kairosdb.util.Tags;

import java.util.*;

public class DataPointSet
{
	private final String m_name;
	private final ImmutableSortedMap.Builder<String, String> m_tags;
	private final List<DataPoint> m_dataPoints;

	public DataPointSet(String name)
	{
		m_name = name;
		m_tags = Tags.create();
		m_dataPoints = new ArrayList<>();
	}

	public DataPointSet(String mName, Map<String, String> tags, List<DataPoint> dataPoints)
	{
		this.m_name = mName;
		this.m_tags = Tags.create().putAll(tags);
		this.m_dataPoints = new ArrayList<>(dataPoints);
	}


	public void addTag(String name, String value)
	{
		m_tags.put(name, value);
	}

	@Override
	public String toString()
	{
		return "DataPointSet{" +
				"m_name='" + m_name + '\'' +
				", m_tags=" + m_tags +
				", m_dataPoints=" + m_dataPoints +
				'}';
	}

	public void addDataPoint(DataPoint dp)
	{
		m_dataPoints.add(dp);
	}

	public String getName() { return (m_name); }

	public ImmutableSortedMap<String, String> getTags()
	{
		return m_tags.build();
	}

	public List<DataPoint> getDataPoints()
	{
		return (Collections.unmodifiableList(m_dataPoints));
	}


	@Override
	public boolean equals(Object o)
	{
		if (this == o) return true;
		if (!(o instanceof DataPointSet)) return false;

		DataPointSet that = (DataPointSet) o;

		return m_name.equals(that.m_name) && m_tags.equals(that.m_tags) && m_dataPoints.equals(that.m_dataPoints);

	}

	@Override
	public int hashCode()
	{
		int result = m_name.hashCode();
		result = 31 * result + m_tags.hashCode();
		result = 31 * result + m_dataPoints.hashCode();
		return result;
	}
}
