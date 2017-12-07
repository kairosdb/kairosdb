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
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.scheduler.KairosDBJob;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;
import static org.quartz.TriggerBuilder.newTrigger;

public class MetricReporterService implements KairosDBJob {
    public static final Logger logger = LoggerFactory.getLogger(MetricReporterService.class);

    public static final String HOSTNAME = "HOSTNAME";
    public static final String SCHEDULE_PROPERTY = "kairosdb.reporter.schedule";

    private MetricReportingConfiguration config;
    private KairosDatastore m_datastore;
    private List<KairosMetricReporter> m_reporters;
    private final String m_hostname;
    private final String m_schedule;

    @Inject
    public MetricReporterService(MetricReportingConfiguration config, KairosDatastore datastore,
                                 List<KairosMetricReporter> reporters,
                                 @Named(SCHEDULE_PROPERTY) String schedule,
                                 @Named(HOSTNAME) String hostname) {
        this.config = checkNotNull(config);
        m_datastore = checkNotNull(datastore);
        m_hostname = checkNotNullOrEmpty(hostname);
        m_reporters = reporters;
        m_schedule = schedule;
    }

    @Override
    public Trigger getTrigger() {
        return (newTrigger()
                .withIdentity(this.getClass().getSimpleName())
                .withSchedule(CronScheduleBuilder.cronSchedule(m_schedule)) //Schedule to run every minute
                .build());
    }

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        if (config.isEnabled()) {
            logger.debug("Reporting metrics");
            long timestamp = System.currentTimeMillis();
            try {
                for (KairosMetricReporter reporter : m_reporters) {
                    List<DataPointSet> dpList = reporter.getMetrics(timestamp);
                    for (DataPointSet dataPointSet : dpList) {
                        for (DataPoint dataPoint : dataPointSet.getDataPoints()) {
                            logger.debug("Storing internal metric {} {} = {}", dataPointSet.getName(),
                                    dataPointSet.getTags(), dataPoint);
                            m_datastore.putDataPoint(dataPointSet.getName(), dataPointSet.getTags(), dataPoint);
                        }
                    }
                }
            } catch (Throwable e) {
                // prevent the thread from dying
                logger.error("Reporter service error", e);
            }
        } else {
            logger.debug("Metric reporting is disabled");
        }
    }
}