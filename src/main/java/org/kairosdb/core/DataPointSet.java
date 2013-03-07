// KairosDB2
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
