package org.kairosdb.datastore.remote;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.scheduler.KairosDBJob;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.quartz.TriggerBuilder.newTrigger;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 6/3/13
 Time: 4:22 PM
 To change this template use File | Settings | File Templates.
 */
public class RemoteSendJob implements KairosDBJob
{
	public static final Logger logger = LoggerFactory.getLogger(RemoteSendJob.class);
	public static final String SCHEDULE = "kairosdb.datastore.remote.schedule";

	private String m_schedule;
	private RemoteDatastore m_datastore;

	@Inject
	public RemoteSendJob(@Named(SCHEDULE) String schedule, RemoteDatastore datastore)
	{
		m_schedule = schedule;
		m_datastore = datastore;
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
	public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException
	{
		try
		{
			logger.debug("Sending remote data");
			m_datastore.sendData();
			logger.debug("Finished sending remote data");
		}
		catch (IOException e)
		{
			logger.error("Unable to send remote data", e);
			throw new JobExecutionException("Unable to send remote data: "+e.getMessage());
		}
	}
}
