/*
 * Copyright 2016 KairosDB Authors
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
import org.kairosdb.core.datastore.Order;
import org.kairosdb.core.datastore.QueryPlugin;

import java.util.Collections;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static org.kairosdb.util.Preconditions.requireNonNullOrEmpty;

public class DatastoreMetricQueryImpl implements DatastoreMetricQuery
{
	private String m_name;
	private SetMultimap<String, String> m_tags;
	private long m_startTime;
	private long m_endTime;


	public DatastoreMetricQueryImpl(String name, SetMultimap<String, String> tags,
			long startTime, long endTime)
	{
		m_name = requireNonNullOrEmpty(name);
		m_tags = requireNonNull(tags);
		m_startTime = startTime;
		m_endTime = endTime;
	}

	@Override
	public String getName()
	{
		return (m_name);
	}

	@Override
	public String getAlias()
	{
		return null;
	}

	@Override
	public SetMultimap<String, String> getTags()
	{
		return (m_tags);
	}

	@Override
	public boolean isExplicitTags()
	{
		return false;
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

	@Override
	public int getLimit()
	{
		return 0;  //To change body of implemented methods use File | Settings | File Templates.
	}

	@Override
	public Order getOrder()
	{
		return Order.ASC;
	}

	@Override
	public List<QueryPlugin> getPlugins()
	{
		return Collections.emptyList();
	}

	@Override
	public void addPlugin(QueryPlugin plugin)
	{

	}

}
