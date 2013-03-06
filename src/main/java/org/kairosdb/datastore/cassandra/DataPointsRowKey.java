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
package org.kairosdb.datastore.cassandra;

import java.util.SortedMap;
import java.util.TreeMap;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 1/11/13
 Time: 10:06 PM
 To change this template use File | Settings | File Templates.
 */
public class DataPointsRowKey
{
	private String m_metricName;
	private long m_timestamp;
	private SortedMap<String, String> m_tags;

	public DataPointsRowKey(String metricName, long timestamp)
	{
		this(metricName, timestamp, new TreeMap<String, String>());
	}

	public DataPointsRowKey(String metricName, long timestamp,
			SortedMap<String, String> tags)
	{
		m_metricName = metricName;
		m_timestamp = timestamp;
		m_tags = tags;
	}

	public void addTag(String name, String value)
	{
		m_tags.put(name, value);
	}

	public String getMetricName()
	{
		return m_metricName;
	}

	public SortedMap<String, String> getTags()
	{
		return m_tags;
	}

	public long getTimestamp()
	{
		return m_timestamp;
	}

	@SuppressWarnings("RedundantIfStatement")
	@Override
	public boolean equals(Object o)
	{
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		DataPointsRowKey that = (DataPointsRowKey) o;

		if (m_timestamp != that.m_timestamp) return false;
		if (!m_metricName.equals(that.m_metricName)) return false;
		if (!m_tags.equals(that.m_tags)) return false;

		return true;
	}

	@Override
	public int hashCode()
	{
		int result = m_metricName.hashCode();
		result = 31 * result + (int) (m_timestamp ^ (m_timestamp >>> 32));
		result = 31 * result + m_tags.hashCode();
		return result;
	}

	@Override
	public String toString()
	{
		return "DataPointsRowKey{" +
				"m_metricName='" + m_metricName + '\'' +
				", m_timestamp=" + m_timestamp +
				", m_tags=" + m_tags +
				'}';
	}
}
