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

package org.kairosdb.datastore.remote;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.scheduler.KairosDBJob;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

import static org.quartz.TriggerBuilder.newTrigger;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 6/3/13
 Time: 4:22 PM
 To change this template use File | Settings | File Templates.
 */
@DisallowConcurrentExecution
@SuppressWarnings("deprecation")
public class RemoteSendJob implements KairosDBJob
{
	public static final Logger logger = LoggerFactory.getLogger(RemoteSendJob.class);
	public static final String SCHEDULE = "kairosdb.datastore.remote.schedule";
	public static final String DELAY = "kairosdb.datastore.remote.random_delay";

	private String m_schedule;
	private int m_delay;
	private Random m_rand;
	private RemoteDatastore m_datastore;

	@Inject
	public RemoteSendJob(@Named(SCHEDULE) String schedule,
			@Named(DELAY) int delay, RemoteDatastore datastore)
	{
		m_schedule = schedule;
		m_delay = delay;
		m_datastore = datastore;

		m_rand = new Random(System.currentTimeMillis());
	}

	@Override
	public Trigger getTrigger()
	{
		return (newTrigger()
				.withIdentity(this.getClass().getSimpleName())
				.withSchedule(CronScheduleBuilder.cronSchedule(m_schedule))
				.build());
	}

	@Override
	public void interrupt()
	{
	}

	@Override
	public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException
	{
		if (m_delay != 0)
		{
			int delay = m_rand.nextInt(m_delay);

			try
			{
				Thread.sleep(delay * 1000L);
			}
			catch (InterruptedException e)
			{
				logger.warn("Sleep delay interrupted", e);
			}
		}

		try
		{
			logger.debug("Sending remote data");
			m_datastore.sendData();
			logger.debug("Finished sending remote data");
		}
		catch (Exception e)
		{
			logger.error("Unable to send remote data", e);
			throw new JobExecutionException("Unable to send remote data: "+e.getMessage());
		}
	}
}
