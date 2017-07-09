package org.kairosdb.util;

/**
 Created by bhawkins on 1/26/17.
 */
public class SimpleStats
{
	private long m_min;
	private long m_max;
	private long m_sum;
	private long m_count;
	private final Object m_dataLock = new Object();

	public SimpleStats()
	{
		clear();
	}

	public void addValue(long value)
	{
		synchronized (m_dataLock)
		{
			m_min = Math.min(m_min, value);
			m_max = Math.max(m_max, value);
			m_sum += value;
			m_count++;
		}
	}

	private void clear()
	{
		m_min = Long.MAX_VALUE;
		m_max = Long.MIN_VALUE;
		m_sum = 0;
		m_count = 0;
	}

	/**
	 Not thread safe
	 @return
	 */
	public long getCount()
	{
		return m_count;
	}

	public Data getAndClear()
	{
		synchronized (m_dataLock)
		{
			Data ret;
			if (m_count != 0)
				ret = new Data(m_min, m_max, m_sum, m_count, ((double)m_sum)/((double)m_count));
			else
				ret = new Data(0, 0, 0, 0, 0.0);

			clear();
			return ret;
		}
	}

	public static class Data
	{
		public final long min;
		public final long max;
		public final long sum;
		public final long count;
		public final double avg;

		public Data(long min, long max, long sum, long count, double avg)
		{
			this.min = min;
			this.max = max;
			this.sum = sum;
			this.count = count;
			this.avg = avg;
		}
	}
}
