package org.kairosdb.datastore.remote;

import org.kairosdb.core.scheduler.KairosDBJob;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Trigger;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 6/3/13
 Time: 4:22 PM
 To change this template use File | Settings | File Templates.
 */
public class RemoteSendJob implements KairosDBJob
{
	@Override
	public Trigger getTrigger()
	{
		return null;  //To change body of implemented methods use File | Settings | File Templates.
	}

	@Override
	public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException
	{
		//To change body of implemented methods use File | Settings | File Templates.
	}
}
