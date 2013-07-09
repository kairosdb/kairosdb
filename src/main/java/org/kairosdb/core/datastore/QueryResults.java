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

package org.kairosdb.core.datastore;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class QueryResults
{
	private final QueryQueuingManager m_queuingManager;
	private final String m_queryHash;

	private List<DataPointGroup> m_dataPoints;

	public QueryResults(QueryQueuingManager queuingManager, String queryHash)
	{
		m_queuingManager = checkNotNull(queuingManager);
		m_queryHash = checkNotNull(queryHash);
	}

	public void addDataPoints(List<DataPointGroup> dataPoints)
	{
		m_dataPoints = dataPoints;
	}

	public List<DataPointGroup> getDataPoints()
	{
		return (m_dataPoints);
	}

	public void close()
	{
		for (DataPointGroup dataPoint : m_dataPoints)
		{
			dataPoint.close();
		}

		m_queuingManager.done(m_queryHash);
	}
}
