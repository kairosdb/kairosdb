package org.kairosdb.core.datastore;

import java.util.List;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 6/27/13
 Time: 2:54 PM
 To change this template use File | Settings | File Templates.
 */
public class QueryResults
{
	List<DataPointGroup> m_dataPoints;

	public QueryResults(List<DataPointGroup> dataPoints)
	{
		m_dataPoints = dataPoints;
	}

	public List<DataPointGroup> getDataPoints()
	{
		return (m_dataPoints);
	}
}
