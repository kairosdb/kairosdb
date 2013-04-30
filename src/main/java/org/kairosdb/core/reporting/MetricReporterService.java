/*
 * Copyright 2013 Proofpoint Inc.
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

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.yammer.metrics.core.*;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.KairosDBService;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.exception.KariosDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;

public class MetricReporterService extends AbstractPollingReporter implements MetricProcessor<Datastore>, KairosDBService
{
	public static final Logger logger = LoggerFactory.getLogger(MetricReporterService.class);

	public static final String REPORTER_PERIOD = "kairosdb.reporter.period";
	public static final String REPORTER_PERIOD_UNIT = "kairosdb.reporter.period_unit";

	private Datastore datastore;
	private KairosMetricRegistry registry;
	private int period;
	private TimeUnit periodUnit;
	private long timestamp;

	@Inject
	public MetricReporterService(Datastore datastore,
	                             KairosMetricRegistry registry,
	                             @Named(REPORTER_PERIOD) int period,
	                             @Named(REPORTER_PERIOD_UNIT) String periodUnit)
	{
		super(registry, "KairosReporter");
		checkArgument(period > 0, "Reporting period must be greater than 0.");
		checkNotNullOrEmpty(periodUnit, "Reporting period unit is not defined.");

		this.datastore = checkNotNull(datastore);
		this.registry = checkNotNull(registry);
		this.period = period;
		this.periodUnit = TimeUnit.valueOf(periodUnit.toUpperCase());
	}

	@Override
	public void start() throws KariosDBException
	{
		start(period, periodUnit);
	}

	@Override
	public void stop()
	{
		shutdown();
	}

	@Override
	public void run()
	{
		logger.debug("Reporting metrics");
		timestamp = System.currentTimeMillis();
		try
		{
			for (Map.Entry<String, SortedMap<MetricName, Metric>> entry : registry.groupedMetrics(
					MetricPredicate.ALL).entrySet())
			{
				for (Map.Entry<MetricName, Metric> subEntry : entry.getValue().entrySet())
				{
					try
					{
						subEntry.getValue().processWith(this, subEntry.getKey(), datastore);
					}
					catch (Exception e)
					{
						logger.error("Could not report metrics", e);
					}
				}
			}

			Runtime runtime = Runtime.getRuntime();
			Map<String, String> tags = new HashMap<String, String>();
			tags.put("host", "server");
			datastore.putDataPoints(new DataPointSet("kairosdb.jvm.free_memory",
					tags, Collections.singletonList(new DataPoint(timestamp, runtime.freeMemory()))));
			datastore.putDataPoints(new DataPointSet("kairosdb.jvm.total_memory",
					tags, Collections.singletonList(new DataPoint(timestamp, runtime.totalMemory()))));
			datastore.putDataPoints(new DataPointSet("kairosdb.jvm.max_memory",
					tags, Collections.singletonList(new DataPoint(timestamp, runtime.maxMemory()))));
			datastore.putDataPoints(new DataPointSet("kairosdb.jvm.thread_count",
					tags, Collections.singletonList(new DataPoint(timestamp, getThreadCount()))));
		}
		catch (Throwable e)
		{
			// prevent the thread from dying
			logger.error("Reporter service error", e);
		}
	}

	@Override
	public void processMeter(MetricName metricName, Metered metered, Datastore datastore) throws Exception
	{
		//todo
	}

	@Override
	public void processCounter(MetricName metricName, Counter counter, Datastore datastore) throws Exception
	{
		datastore.putDataPoints(new DataPointSet(registry.getKairosName(metricName),
				registry.getTags(metricName),
				Collections.singletonList(new DataPoint(timestamp, counter.count()))));
	}

	@Override
	public void processHistogram(MetricName metricName, Histogram histogram, Datastore datastore) throws Exception
	{
		//todo
	}

	@Override
	public void processTimer(MetricName metricName, Timer timer, Datastore datastore) throws Exception
	{
		//todo
	}

	@Override
	public void processGauge(MetricName metricName, Gauge<?> gauge, Datastore datastore) throws Exception
	{
		// todo what kind of gauges do we want to support?
	}

	private int getThreadCount()
	{
		ThreadGroup tg = Thread.currentThread().getThreadGroup();
		while (tg.getParent() != null)
		{
			tg = tg.getParent();
		}

		return tg.activeCount();
	}
}