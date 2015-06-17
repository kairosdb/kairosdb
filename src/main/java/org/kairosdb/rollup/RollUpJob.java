package org.kairosdb.rollup;

import com.google.inject.Inject;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.http.rest.json.Metric;
import org.kairosdb.core.scheduler.KairosDBSchedulerImpl;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.inject.internal.util.$Preconditions.checkNotNull;
import static com.google.inject.internal.util.$Preconditions.checkState;
import static org.quartz.TriggerBuilder.newTrigger;

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
		RollUpTask task = (RollUpTask) dataMap.get("task");
		checkState(task != null, "Task was null");

		long now = System.currentTimeMillis();
//		QueryMetric query = new QueryMetric(task.getStartTime().getTimeRelativeTo(now), task.getEndTime().getTimeRelativeTo(now));
//		datastore.createQuery(task.)

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
