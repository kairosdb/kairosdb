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
