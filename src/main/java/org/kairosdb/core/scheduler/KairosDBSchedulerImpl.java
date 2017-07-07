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
package org.kairosdb.core.scheduler;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.kairosdb.core.KairosDBService;
import org.kairosdb.core.exception.KairosDBException;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.quartz.JobBuilder.newJob;

public class KairosDBSchedulerImpl implements KairosDBService, KairosDBScheduler
{
	private static final Logger log = LoggerFactory.getLogger(KairosDBSchedulerImpl.class);

	private final Scheduler scheduler;
	private final Injector guice;

	@Inject
	public KairosDBSchedulerImpl(Injector guice) throws SchedulerException
	{
		this.guice = guice;

		Properties props = new Properties();
		props.setProperty("org.quartz.threadPool.threadCount", "4");
		props.setProperty(StdSchedulerFactory.PROP_SCHED_SKIP_UPDATE_CHECK, "true");

		StdSchedulerFactory factory = new StdSchedulerFactory(props);
		scheduler = factory.getScheduler();
		scheduler.setJobFactory(new KairosDBJobFactory(guice));
	}

	@SuppressWarnings("unchecked")
	@Override
	public void start() throws KairosDBException
	{
		try
		{
			scheduler.start();

			for (Key<?> key : guice.getAllBindings().keySet())
			{
				Class bindingClass = key.getTypeLiteral().getRawType();
				if (KairosDBJob.class.isAssignableFrom(bindingClass))
				{
					log.info("Starting job: "+bindingClass.getName());
					KairosDBJob job = (KairosDBJob) guice.getInstance(bindingClass);
					JobDetail jobDetail = newJob(job.getClass())
							.withIdentity(job.getClass().getName()).build();

					scheduler.scheduleJob(jobDetail, job.getTrigger());
				}
			}

			for (String groupName : scheduler.getJobGroupNames())
			{
				for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher.jobGroupEquals(groupName)))
				{
					String jobName = jobKey.getName();
					List<Trigger> triggers = (List<Trigger>) scheduler.getTriggersOfJob(jobKey);
					Date nextFireTime = triggers.get(0).getNextFireTime();
					log.info("*** Scheduled job " + jobName + " to execute next on " + nextFireTime);
				}
			}
		}
		catch (SchedulerException e)
		{
			throw new KairosDBException("Failed to start " + getClass().getName(), e);
		}
	}

	@Override
	public void stop()
	{
		try
		{
			scheduler.shutdown(true);
		}
		catch (SchedulerException e)
		{
			log.error("Failed to start " + getClass().getName(), e);
		}
	}

	@Override
	public void schedule(JobDetail jobDetail, Trigger trigger) throws KairosDBException
	{
		checkNotNull(jobDetail);
		checkNotNull(trigger);

		try
		{
			scheduler.scheduleJob(jobDetail, trigger);
		}
		catch (SchedulerException e)
		{
			throw new KairosDBException("Failed to schedule trigger " + jobDetail, e);
		}
	}

	@Override
	public void cancel(JobKey jobKey) throws KairosDBException
	{
		checkNotNull(jobKey);

		try
		{
			scheduler.deleteJob(jobKey);
		}
		catch (SchedulerException e)
		{
			throw new KairosDBException("Failed to delete job " + jobKey, e);
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public Set<String> getScheduledJobIds() throws KairosDBException
	{
		Set<String> scheduledJobs = new HashSet<String>();
		try
		{
			for (String groupName : scheduler.getJobGroupNames())
			{
				for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher.jobGroupEquals(groupName)))
				{
					scheduledJobs.add(jobKey.getName());
				}
			}
		}
		catch (SchedulerException e)
		{
			throw new KairosDBException("Could not get scheduled jobs." + e);
		}

		return scheduledJobs;
	}
}