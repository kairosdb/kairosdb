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

package org.kairosdb.datastore;

import org.kairosdb.core.datastore.DatastoreMetricQuery;

import java.util.Map;

public class DatastoreMetricQueryImpl implements DatastoreMetricQuery
{
	private String m_name;
	private Map<String, String> m_tags;
	private long m_startTime;
	private long m_endTime;


	public DatastoreMetricQueryImpl(String name, Map<String, String> tags,
			long startTime, long endTime)
	{
		m_name = name;
		m_tags = tags;
		m_startTime = startTime;
		m_endTime = endTime;
	}

	@Override
	public String getName()
	{
		return (m_name);
	}

	@Override
	public Map<String, String> getTags()
	{
		return (m_tags);
	}

	@Override
	public long getStartTime()
	{
		return (m_startTime);
	}

	@Override
	public long getEndTime()
	{
		return (m_endTime);
	}
}
