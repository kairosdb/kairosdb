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
package org.kairosdb.core.jobs;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.scheduler.KairosDBJob;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.quartz.TriggerBuilder.newTrigger;

public class CacheFileCleaner implements KairosDBJob
{
	public static final Logger logger = LoggerFactory.getLogger(CacheFileCleaner.class);
	public static final String CLEANING_SCHEDULE = "kairosdb.query_cache.cache_file_cleaner_schedule";

	private final KairosDatastore datastore;
	private String schedule;

	@Inject
	public CacheFileCleaner(@Named(CLEANING_SCHEDULE) String schedule, KairosDatastore datastore)
	{
		this.datastore = datastore;
		this.schedule = schedule;
	}

	@Override
	public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException
	{
		logger.debug("Executing job...");
		datastore.cleanCacheDir(true);
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

	@Override
	public void interrupt()
	{
	}
}