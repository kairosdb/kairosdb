package org.kairosdb.events;

import org.kairosdb.datastore.cassandra.DataPointsRowKey;

/**
 Created by bhawkins on 10/15/16.
 */
public class RowKeyEvent
{
	private final String m_metricName;
	private final DataPointsRowKey m_rowKey;
	private final int m_rowKeyTtl;

	public RowKeyEvent(String metricName, DataPointsRowKey rowKey, int rowKeyTtl)
	{
		m_metricName = metricName;
		m_rowKey = rowKey;
		m_rowKeyTtl = rowKeyTtl;
	}

	public String getMetricName()
	{
		return m_metricName;
	}

	public DataPointsRowKey getRowKey()
	{
		return m_rowKey;
	}

	public int getRowKeyTtl()
	{
		return m_rowKeyTtl;
	}
}
