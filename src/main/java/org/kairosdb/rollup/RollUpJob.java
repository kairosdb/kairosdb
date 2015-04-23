package org.kairosdb.rollup;

import org.kairosdb.core.scheduler.KairosDBJob;
import org.kairosdb.core.scheduler.KairosDBSchedulerImpl;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.quartz.TriggerBuilder.newTrigger;

public class RollUpJob implements KairosDBJob
{
	private static final Logger log = LoggerFactory.getLogger(KairosDBSchedulerImpl.class);
	private final RollUpTask task;

	private boolean interrupted;

	public RollUpJob(RollUpTask task)
	{
		this.task = checkNotNull(task);
	}

	@Override
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
		log.info("Execution job " + task.getMetricName());
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
