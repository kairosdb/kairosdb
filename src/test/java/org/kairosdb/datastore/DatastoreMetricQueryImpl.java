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

package org.kairosdb.datastore;

import com.google.common.collect.SetMultimap;
import org.kairosdb.core.datastore.DatastoreMetricQuery;

import java.util.Map;

public class DatastoreMetricQueryImpl implements DatastoreMetricQuery
{
	private String m_name;
	private SetMultimap<String, String> m_tags;
	private long m_startTime;
	private long m_endTime;


	public DatastoreMetricQueryImpl(String name, SetMultimap<String, String> tags,
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
	public SetMultimap<String, String> getTags()
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
