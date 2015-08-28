package org.kairosdb.rollup;

import org.kairosdb.core.scheduler.KairosDBSchedulerImpl;
import org.quartz.InterruptableJob;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.inject.internal.util.$Preconditions.checkState;

public class RollUpJob implements InterruptableJob
{
	private static final Logger log = LoggerFactory.getLogger(KairosDBSchedulerImpl.class);
//	private final KairosDatastore datastore;

	private boolean interrupted;

	public RollUpJob()
	{
	}

	//	public RollUpJob(KairosDatastore datastore)
//	{
//		this.datastore = checkNotNull(datastore);
//	}

	@Override
	public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException
	{
		JobDataMap dataMap = jobExecutionContext.getMergedJobDataMap();
		RollupTask task = (RollupTask) dataMap.get("task");
		checkState(task != null, "Task was null");

		long now = System.currentTimeMillis();
//		QueryMetric query = new QueryMetric(task.getStartTime().getTimeRelativeTo(now), task.getEndTime().getTimeRelativeTo(now));
//		datastore.createQuery(task.)

		//		log.info("Executing job " + task.getMetricName());

		// todo
//		if (interrupted)
//			break;
	}

	@Override
	public void interrupt()
	{
		interrupted = true;
	}
}
