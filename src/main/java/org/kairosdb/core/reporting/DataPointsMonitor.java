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
	private static final MetricCounters metrics = MetricSourceManager.getSource(MetricCounters.class);
	private final String m_prefix;

	public interface MetricCounters
	{
		LongCollector countMetric(@Key("metric_name") String metricName);
	}

	@Inject
	public DataPointsMonitor(@Named("kairosdb.metric-prefix") String prefix)
	{
		m_prefix = prefix;
	}


	@Subscribe
	public void dataPoint(DataPointEvent event)
	{
		String metricName = event.getMetricName();

		if (metricName.startsWith(m_prefix))
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
