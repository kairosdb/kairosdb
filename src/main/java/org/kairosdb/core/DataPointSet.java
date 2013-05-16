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

import java.util.*;

public class DataPointSet
{
	private String m_name;
	private SortedMap<String, String> m_tags;
	private List<DataPoint> m_dataPoints;

	public DataPointSet(String name)
	{
		m_name = name;
		m_tags = new TreeMap<String, String>();
		m_dataPoints = new ArrayList<DataPoint>();
	}

	public DataPointSet(String mName, Map<String, String> tags, List<DataPoint> dataPoints)
	{
		this.m_name = mName;
		this.m_tags = new TreeMap<String, String>(tags);
		this.m_dataPoints = new ArrayList<DataPoint>(dataPoints);
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

	public SortedMap<String, String> getTags()
	{
		return (Collections.unmodifiableSortedMap(m_tags));
	}

	public List<DataPoint> getDataPoints()
	{
		return (Collections.unmodifiableList(m_dataPoints));
	}
}
