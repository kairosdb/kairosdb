/*
 * Copyright 2016 KairosDB Authors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.kairosdb.core.reporting;

import com.google.common.collect.ImmutableSortedMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;
import org.kairosdb.core.scheduler.KairosDBJob;
import org.kairosdb.eventbus.FilterEventBus;
import org.kairosdb.eventbus.Publisher;
import org.kairosdb.events.DataPointEvent;
import org.kairosdb.util.Tags;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.List;

import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;
import static org.quartz.TriggerBuilder.newTrigger;

public class MetricReporterService implements KairosDBJob
{
	public static final Logger logger = LoggerFactory.getLogger(MetricReporterService.class);

	public static final String HOSTNAME = "HOSTNAME";
	public static final String SCHEDULE_PROPERTY = "kairosdb.reporter.schedule";
	public static final String REPORTER_TTL = "kairosdb.reporter.ttl";

	private Publisher<DataPointEvent> m_publisher;
	private KairosMetricReporterListProvider m_reporterProvider;
	private final String m_hostname;
	private final String m_schedule;
	private final int m_ttl;

	@Inject
	private LongDataPointFactory m_dataPointFactory = new LongDataPointFactoryImpl();

	@Inject
	public MetricReporterService(FilterEventBus eventBus,
			KairosMetricReporterListProvider reporterProvider,
			@Named(SCHEDULE_PROPERTY) String schedule,
			@Named(HOSTNAME) String hostname,
			@Named(REPORTER_TTL) int ttl)
	{
		m_hostname = checkNotNullOrEmpty(hostname);
		m_reporterProvider = reporterProvider;
		m_schedule = schedule;
		m_ttl = ttl;

		m_publisher = eventBus.createPublisher(DataPointEvent.class);
	}

	private int getThreadCount()
	{
		return ManagementFactory.getThreadMXBean().getThreadCount();
	}

	@Override
	public Trigger getTrigger()
	{
		return (newTrigger()
				.withIdentity(this.getClass().getSimpleName())
				.withSchedule(CronScheduleBuilder.cronSchedule(m_schedule)) //Schedule to run every minute
				.build());
	}

	@Override
	public void interrupt()
	{
	}

	@Override
	public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException
	{
		logger.debug("Reporting metrics");
		long timestamp = System.currentTimeMillis();
		try
		{
			for (KairosMetricReporter reporter : m_reporterProvider.get())
			{
				List<DataPointSet> dpList = reporter.getMetrics(timestamp);
				for (DataPointSet dataPointSet : dpList)
				{
					for (DataPoint dataPoint : dataPointSet.getDataPoints())
					{
						m_publisher.post(new DataPointEvent(dataPointSet.getName(),
								dataPointSet.getTags(), dataPoint, m_ttl));
					}
				}
			}


			Runtime runtime = Runtime.getRuntime();
			ImmutableSortedMap<String, String> tags = Tags.create()
					.put("host", m_hostname).build();
			m_publisher.post(new DataPointEvent("kairosdb.jvm.free_memory",
					tags, m_dataPointFactory.createDataPoint(timestamp, runtime.freeMemory()), m_ttl));
			m_publisher.post(new DataPointEvent("kairosdb.jvm.total_memory",
					tags, m_dataPointFactory.createDataPoint(timestamp, runtime.totalMemory()), m_ttl));
			m_publisher.post(new DataPointEvent("kairosdb.jvm.max_memory",
					tags, m_dataPointFactory.createDataPoint(timestamp, runtime.maxMemory()), m_ttl));
			m_publisher.post(new DataPointEvent("kairosdb.jvm.thread_count",
					tags, m_dataPointFactory.createDataPoint(timestamp, getThreadCount()), m_ttl));
		}
		catch (Throwable e)
		{
			// prevent the thread from dying
			logger.error("Reporter service error", e);
		}
	}
}