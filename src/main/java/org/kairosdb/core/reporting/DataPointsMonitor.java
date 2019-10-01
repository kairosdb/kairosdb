//
// DataPointsMonitor.java
//
// Copyright 2013, NextPage Inc. All rights reserved.
//

package org.kairosdb.core.reporting;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.KairosDBService;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.eventbus.Subscribe;
import org.kairosdb.events.DataPointEvent;
import org.kairosdb.metrics4j.MetricSourceManager;
import org.kairosdb.metrics4j.annotation.Key;
import org.kairosdb.metrics4j.collectors.LongCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;


public class DataPointsMonitor implements KairosDBService
{
	public static final Logger logger = LoggerFactory.getLogger(DataPointsMonitor.class);
	private static final MetricCounters metrics = MetricSourceManager.getSource(MetricCounters.class);
	//public static final String METRIC_NAME = "kairosdb.metric_counters";

	//private volatile ConcurrentMap<String, AtomicInteger> m_metricCounters;

	//@Inject
	//private LongDataPointFactory m_dataPointFactory = new LongDataPointFactoryImpl();

	public interface MetricCounters
	{
		LongCollector countMetric(@Key("metric_name") String metricName);
	}

	@Inject
	public DataPointsMonitor(@Named("HOSTNAME") String hostName)
	{
		//m_metricCounters = new ConcurrentHashMap<String, AtomicInteger>();
	}

	/*private void addCounter(String name, int count)
	{
		AtomicInteger ai = m_metricCounters.get(name);

		if (ai == null)
		{
			ai = new AtomicInteger();
			AtomicInteger mapValue = m_metricCounters.putIfAbsent(name, ai);
			ai = (mapValue != null ? mapValue : ai); //This handles the case if one snuck in on another thread.
		}

		ai.addAndGet(count);
	}*/


	/*private Map<String, AtomicInteger> getAndClearCounters()
	{
		Map<String, AtomicInteger> ret = m_metricCounters;

		m_metricCounters = new ConcurrentHashMap<String, AtomicInteger>();

		return (ret);
	}*/


	/*@Override
	public List<DataPointSet> getMetrics(long now)
	{
		List<DataPointSet> ret = new ArrayList<DataPointSet>();

		Map<String, AtomicInteger> counters = getAndClearCounters();

		for (String name : counters.keySet())
		{
			DataPointSet dps = new DataPointSet(METRIC_NAME);
			dps.addTag("metric_name", name);
			dps.addDataPoint(m_dataPointFactory.createDataPoint(now, counters.get(name).longValue()));

			ret.add(dps);
		}

		return (ret);
	}*/

	@Subscribe
	public void dataPoint(DataPointEvent event)
	{
		String metricName = event.getMetricName();

		if (metricName.startsWith("kairosdb"))
			return; //Skip our own metrics.

		metrics.countMetric(metricName).put(1);
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
