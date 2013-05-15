//
// DataPointsMonitor.java
//
// Copyright 2013, NextPage Inc. All rights reserved.
//

package org.kairosdb.core.reporting;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointListener;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.scheduler.KairosDBJob;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.quartz.TriggerBuilder.newTrigger;

public class DataPointsMonitor implements DataPointListener, KairosDBJob
{
	public static final Logger logger = LoggerFactory.getLogger(DataPointsMonitor.class);
	public static final String METRIC_NAME = "kairosdb.metric_counters";

	private KairosDatastore m_datastore;
	private volatile ConcurrentMap<String, AtomicInteger> m_metricCounters;
	private String m_hostName;

	@Inject
	public DataPointsMonitor(KairosDatastore datastore,
			@Named("HOSTNAME") String hostName)
	{
		m_datastore = datastore;
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

	@Override
	public void dataPoints(DataPointSet pds)
	{
		String metricName = pds.getName();
		if (metricName.equals(METRIC_NAME))
			return; //Skip our own metric.

		int count = pds.getDataPoints().size();

		addCounter(metricName, count);
	}

	private Map<String, AtomicInteger> getAndClearCounters()
	{
		Map<String, AtomicInteger> ret = m_metricCounters;

		m_metricCounters = new ConcurrentHashMap<String, AtomicInteger>();

		return (ret);
	}

	@Override
	public Trigger getTrigger()
	{
		return (newTrigger()
				.withIdentity(this.getClass().getSimpleName())
				.withSchedule(CronScheduleBuilder.cronSchedule("0 */1 * * * ?")) //Schedule to run every minute
				.build());
	}

	@Override
	public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException
	{
		Map<String, AtomicInteger> counters = getAndClearCounters();

		long now = System.currentTimeMillis();
		for (String name : counters.keySet())
		{
			DataPointSet dps = new DataPointSet(METRIC_NAME);
			dps.addTag("host", m_hostName);
			dps.addTag("metric_name", name);
			dps.addDataPoint(new DataPoint(now, counters.get(name).longValue()));
			try
			{
				m_datastore.putDataPoints(dps);
			}
			catch (DatastoreException e)
			{
				logger.error("DataPointMonitor failed adding adding metrics", e);
			}
		}

	}
}
