package org.kairosdb.datastore.cassandra;

import org.kairosdb.util.SimpleStats;

/**
 Created by bhawkins on 1/26/17.
 */
public class BatchStats
{
	private final SimpleStats m_dataPointStats = new SimpleStats();
	private final SimpleStats m_rowKeyStats = new SimpleStats();
	private final SimpleStats m_nameStats = new SimpleStats();

	public BatchStats()
	{
	}

	public void addNameBatch(long count)
	{
		m_nameStats.addValue(count);
	}

	public void addRowKeyBatch(long count)
	{
		m_rowKeyStats.addValue(count);
	}

	public void addDatapointsBatch(long count)
	{
		m_dataPointStats.addValue(count);
	}

	public SimpleStats.Data getDataPointStats()
	{
		return m_dataPointStats.getAndClear();
	}

	public SimpleStats.Data getRowKeyStats()
	{
		return m_rowKeyStats.getAndClear();
	}

	public SimpleStats.Data getNameStats()
	{
		return m_nameStats.getAndClear();
	}
}
