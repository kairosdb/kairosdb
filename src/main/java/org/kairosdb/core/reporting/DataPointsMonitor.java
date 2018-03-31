//
// DataPointsMonitor.java
//
// Copyright 2013, NextPage Inc. All rights reserved.
//

package org.kairosdb.core.reporting;

import org.kairosdb.core.KairosDBService;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.eventbus.Subscribe;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;
import org.kairosdb.events.DataPointEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;


public class DataPointsMonitor implements KairosMetricReporter, KairosDBService
{
	public static final Logger logger = LoggerFactory.getLogger(DataPointsMonitor.class);
	public static final String METRIC_NAME = "kairosdb.metric_counters";

	private volatile ConcurrentMap<String, AtomicInteger> m_metricCounters;
	private String m_hostName;

	@Inject
	private LongDataPointFactory m_dataPointFactory = new LongDataPointFactoryImpl();

	@Inject
	public DataPointsMonitor(@Named("HOSTNAME") String hostName)
	{
		m_metricCounters = new ConcurrentHashMap<String, AtomicInteger>();
		m_hostName = hostName;
	}

	private void addCounter(String name, int count)
	{
		AtomicInteger ai = m_metricCounters.get(name);

		if (ai == null)
		{
			ai = new AtomicInteger();
			AtomicInteger mapValue = m_metricCounters.putIfAbsent(name, ai);
			ai = (mapValue != null ? mapValue : ai); //This handles the case if one snuck in on another thread.
		}

		ai.addAndGet(count);
	}


	private Map<String, AtomicInteger> getAndClearCounters()
	{
		Map<String, AtomicInteger> ret = m_metricCounters;

		m_metricCounters = new ConcurrentHashMap<String, AtomicInteger>();

		return (ret);
	}


	@Override
	public List<DataPointSet> getMetrics(long now)
	{
		List<DataPointSet> ret = new ArrayList<DataPointSet>();

		Map<String, AtomicInteger> counters = getAndClearCounters();

		for (String name : counters.keySet())
		{
			DataPointSet dps = new DataPointSet(METRIC_NAME);
			dps.addTag("host", m_hostName);
			dps.addTag("metric_name", name);
			dps.addDataPoint(m_dataPointFactory.createDataPoint(now, counters.get(name).longValue()));

			ret.add(dps);
		}

		return (ret);
	}

	@Subscribe
	public void dataPoint(DataPointEvent event)
	{
		String metricName = event.getMetricName();

		if (metricName.startsWith("kairosdb"))
			return; //Skip our own metrics.

		addCounter(metricName, 1);
	}

	@Override
	public void start() throws KairosDBException
	{

	}

	@Override
	public void stop()
	{

	}
}
