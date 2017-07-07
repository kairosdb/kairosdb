package org.kairosdb.util;


import java.util.HashMap;
import java.util.Map;

/**
 Contains a map of SimpleStats
 Created by bhawkins on 1/20/17.
 */
public class StatsMap
{
	private Map<String, SimpleStats> m_statsMap;
	private Object m_mapLock = new Object();

	public StatsMap()
	{
		m_statsMap = new HashMap<>();
	}

	public void addMetric(String name, long value)
	{
		synchronized (m_mapLock)
		{
			SimpleStats stats = m_statsMap.get(name);
			if (stats == null)
			{
				stats = new SimpleStats();
				m_statsMap.put(name, stats);
			}

			stats.addValue(value);
		}
	}

	public Map<String, SimpleStats> getStatsMap()
	{
		Map<String, SimpleStats> ret;
		synchronized (m_mapLock)
		{
			ret = m_statsMap;
			m_statsMap = new HashMap<>();
		}

		return ret;
	}
}
