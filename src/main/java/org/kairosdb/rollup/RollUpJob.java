package org.kairosdb.rollup;

import org.kairosdb.core.scheduler.KairosDBSchedulerImpl;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.quartz.TriggerBuilder.newTrigger;

public class RollUpJob implements InterruptableJob
{
	private static final Logger log = LoggerFactory.getLogger(KairosDBSchedulerImpl.class);
	private RollUpTask task;

	private boolean interrupted;

	public RollUpJob()
	{
	}

	public RollUpJob(RollUpTask task)
	{
		this.task = checkNotNull(task);
	}

//	@Override
	public Trigger getTrigger()
	{
		return newTrigger()
				.withIdentity(task.getId() + "-" + this.getClass().getSimpleName())
				.withSchedule(CronScheduleBuilder.cronSchedule(task.getSchedule()))
				.build();
	}

	@Override
	public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException
	{
		JobDataMap dataMap = jobExecutionContext.getMergedJobDataMap();
		RollUpTask task = (RollUpTask) dataMap.get("task");

		log.info("Executing job " + task.getMetricName());

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
