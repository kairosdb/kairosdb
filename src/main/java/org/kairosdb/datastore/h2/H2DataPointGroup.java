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

package org.kairosdb.datastore.h2;

import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datastore.DataPointRow;
import org.kairosdb.datastore.h2.orm.DataPoint_base;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class H2DataPointGroup implements DataPointRow
{
	private String m_name;
	private DataPoint_base.ResultSet m_results;
	private DataPoint_base m_nextDataPoint;
	private Map<String, String> m_tags = new TreeMap<String, String>();

	public H2DataPointGroup(String name, Map<String, String> tags,
			DataPoint_base.ResultSet resultSet)
	{
		m_name = name;
		m_tags = tags;
		m_results = resultSet;
		if (m_results.next())
			m_nextDataPoint = m_results.getRecord();
	}

	public boolean hasNext()
	{
		return (m_nextDataPoint != null);
	}

	public DataPoint next()
	{
		DataPoint ret = null;

		if (m_nextDataPoint != null)
		{
			if (m_nextDataPoint.isLongValueNull())
				ret = new DataPoint(m_nextDataPoint.getTimestamp().getTime(),
						m_nextDataPoint.getDoubleValue());
			else
				ret = new DataPoint(m_nextDataPoint.getTimestamp().getTime(),
						m_nextDataPoint.getLongValue());

			if (m_results.next())
				m_nextDataPoint = m_results.getRecord();
			else
				m_nextDataPoint = null;
		}

		return (ret);
	}

	@Override
	public void remove()
	{
	}

	@Override
	public String getName()
	{
		return (m_name);
	}

	@Override
	public Set<String> getTagNames()
	{
		return (m_tags.keySet());
	}

	@Override
	public String getTagValue(String tag)
	{
		return (m_tags.get(tag));
	}

	@Override
	public void close()
	{
		m_results.close();
	}
}
