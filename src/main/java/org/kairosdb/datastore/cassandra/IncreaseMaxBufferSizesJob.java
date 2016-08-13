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
package org.kairosdb.datastore.cassandra;

import com.google.inject.Inject;
import org.kairosdb.core.scheduler.KairosDBJob;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;

import static org.quartz.TriggerBuilder.newTrigger;

public class IncreaseMaxBufferSizesJob implements KairosDBJob
{
	public static final Logger logger = LoggerFactory.getLogger(IncreaseMaxBufferSizesJob.class);
	public static final String SCHEDULE = "kairosdb.datastore.cassandra.increase_buffer_size_schedule";

	private final CassandraDatastore datastore;
	private String schedule;

	@Inject
	public IncreaseMaxBufferSizesJob(@Named(SCHEDULE) String schedule, CassandraDatastore datastore)
	{
		this.datastore = datastore;
		this.schedule = schedule;
	}

	@Override
	public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException
	{
		logger.debug("Executing job...");
		datastore.increaseMaxBufferSizes();
		logger.debug("Job Completed");
	}

	@Override
	public Trigger getTrigger()
	{
		return newTrigger()
				.withIdentity(this.getClass().getSimpleName())
				.withSchedule(CronScheduleBuilder.cronSchedule(schedule))
				.build();
	}
}