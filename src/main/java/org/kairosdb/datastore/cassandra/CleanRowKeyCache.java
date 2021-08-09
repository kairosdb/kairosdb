package org.kairosdb.datastore.cassandra;

import com.google.inject.Inject;
import org.kairosdb.core.scheduler.KairosDBJob;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Trigger;

import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 1/7/14
 Time: 2:58 PM
 This cleans out keys from the cache that are no longer part of the current time
 bucket.
 */
public class CleanRowKeyCache implements KairosDBJob
{
	private CassandraDatastore m_datastore;

	@Inject
	public CleanRowKeyCache(CassandraDatastore datastore)
	{
		m_datastore = datastore;
	}

	@Override
	public Trigger getTrigger()
	{
		return newTrigger()
				.withIdentity(this.getClass().getSimpleName())
				.withSchedule(simpleSchedule()
						.withIntervalInHours(1)
						.repeatForever())
				.build();
	}

	@Override
	public void interrupt()
	{
	}

	@Override
	public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException
	{
		m_datastore.cleanRowKeyCache();
	}
}
