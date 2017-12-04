package org.kairosdb.events;

/**
 Created by bhawkins on 3/25/17.
 */
public class BatchReductionEvent
{
	private final int m_batchSize;

	public BatchReductionEvent(int batchSize)
	{
		m_batchSize = batchSize;
	}

	public int getBatchSize()
	{
		return m_batchSize;
	}
}
