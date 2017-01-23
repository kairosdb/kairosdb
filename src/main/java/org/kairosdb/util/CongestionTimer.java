package org.kairosdb.util;

import com.google.common.collect.TreeMultiset;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

/**
 Created by bhawkins on 10/27/16.
 */
public class CongestionTimer
{
	private volatile int m_taskPerBatch;
	private final DescriptiveStatistics m_stats;
	private final Object m_statsLock;

	public CongestionTimer(int taskPerBatch)
	{
		m_taskPerBatch = taskPerBatch;
		m_stats = new DescriptiveStatistics();
		m_statsLock = new Object();
	}

	public void setTaskPerBatch(int taskPerBatch)
	{
		m_taskPerBatch = taskPerBatch;
	}

	public TimerStat reportTaskTime(long time)
	{
		synchronized(m_statsLock)
		{
			m_stats.addValue(time);

			if (m_stats.getN() == m_taskPerBatch)
			{
				TimerStat ts = new TimerStat(m_stats.getMin(), m_stats.getMax(),
						m_stats.getMean(), m_stats.getPercentile(50));

				m_stats.clear();


				return ts;
			}
		}

		return null;
	}

	public static class TimerStat
	{
		public final double min;
		public final double max;
		public final double avg;
		public final double median;


		public TimerStat(double min, double max, double avg, double median)
		{
			this.min = min;
			this.max = max;
			this.avg = avg;
			this.median = median;
		}
	}
}
