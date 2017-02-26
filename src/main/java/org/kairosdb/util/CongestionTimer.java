package org.kairosdb.util;


/**
 Created by bhawkins on 10/27/16.
 */
public class CongestionTimer
{
	private volatile int m_taskPerBatch;
	private final SimpleStats m_stats;
	private final Object m_statsLock;

	public CongestionTimer(int taskPerBatch)
	{
		m_taskPerBatch = taskPerBatch;
		m_stats = new SimpleStats();
		m_statsLock = new Object();
	}

	public void setTaskPerBatch(int taskPerBatch)
	{
		m_taskPerBatch = taskPerBatch;
	}

	public SimpleStats.Data reportTaskTime(long time)
	{
		synchronized(m_statsLock)
		{
			m_stats.addValue(time);

			if (m_stats.getCount() == m_taskPerBatch)
			{
				SimpleStats.Data data = m_stats.getAndClear();

				return data;
			}
		}

		return null;
	}
}
