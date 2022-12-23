package org.kairosdb.rollup;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;
import org.kairosdb.core.datapoints.StringDataPointFactory;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.scheduler.KairosDBSchedulerImpl;
import org.kairosdb.eventbus.FilterEventBus;
import org.kairosdb.eventbus.Publisher;
import org.kairosdb.events.DataPointEvent;
import org.kairosdb.metrics4j.MetricSourceManager;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public class RollUpJob implements InterruptableJob
{
	private static final Logger log = LoggerFactory.getLogger(KairosDBSchedulerImpl.class);
	private static final RollupStats stats = MetricSourceManager.getSource(RollupStats.class);

	private static final String ROLLUP_TIME = "kairosdb.rollup.execution-time";

	private boolean interrupted;
	private LongDataPointFactory longDataPointFactory = new LongDataPointFactoryImpl();
	private StringDataPointFactory stringDataPointFactory = new StringDataPointFactory();

	public RollUpJob()
	{
	}

	@SuppressWarnings("ConstantConditions")
	@Override
	public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException
	{
		JobDataMap dataMap = jobExecutionContext.getMergedJobDataMap();
		processRollups(jobExecutionContext, dataMap);
	}

	private void processRollups(JobExecutionContext jobExecutionContext, JobDataMap dataMap)
	{
		try
		{
			RollupTask task = (RollupTask) dataMap.get("task");
			FilterEventBus eventBus = (FilterEventBus) dataMap.get("eventBus");
			KairosDatastore datastore = (KairosDatastore) dataMap.get("datastore");
			String hostName = (String) dataMap.get("hostName");
			RollupTaskStatusStore statusStore = (RollupTaskStatusStore) dataMap.get("statusStore");
			checkState(task != null, "Task was null");
			checkState(eventBus != null, "EventBus was null");
			checkState(datastore != null, "Datastore was null");
			checkState(hostName != null, "hostname was null");
			checkState(statusStore != null, "statusStore was null");

			if (isJobAlreadyRunning(jobExecutionContext, task.getName())) return;

			Publisher<DataPointEvent> publisher = eventBus.createPublisher(DataPointEvent.class);

			for (Rollup rollup : task.getRollups())
			{
				log.info("Executing Rollup Task: " + task.getName() + " for Rollup  " + rollup.getSaveAs());

				RollupTaskStatus status = new RollupTaskStatus(jobExecutionContext.getNextFireTime(), hostName);
				RollupProcessor processor = new RollupProcessorImpl(datastore);

				if (interrupted){
					processor.interrupt();
					break;
				}

				for (QueryMetric queryMetric : rollup.getQueryMetrics())
				{
					if (interrupted)
					{
						processor.interrupt();
						break;
					}
					boolean success = true;
					long executionStartTime = System.currentTimeMillis();
					long executionLength = 0L;
					try
					{
						long dpCount = processor.process(statusStore, task, queryMetric, rollup.getTimeZone());
						executionLength = System.currentTimeMillis() - executionStartTime;
						status.addStatus(RollupTaskStatus.createQueryMetricStatus(queryMetric.getName(), System.currentTimeMillis(), dpCount, executionLength));
					}
					catch (DatastoreException e)
					{
						success = false;
						log.error("Failed to execute query for roll-up task: " + task.getName() + " roll-up: " + rollup.getSaveAs(), e);
						status.addStatus(RollupTaskStatus.createErrorQueryMetricStatus(queryMetric.getName(), System.currentTimeMillis(), ExceptionUtils.getStackTrace(e), 0));
					}
					catch (RuntimeException e)
					{
						success = false;
						log.error("Failed to roll-up task: " + task.getName() + " roll-up: " + rollup.getSaveAs(), e);
						status.addStatus(RollupTaskStatus.createErrorQueryMetricStatus(queryMetric.getName(), System.currentTimeMillis(), ExceptionUtils.getStackTrace(e), 0));
					}
					finally
					{
						log.info("Rollup Task: " + task.getName() + " for Rollup  " + rollup.getSaveAs() + " completed");
						stats.executionTime(rollup.getSaveAs(),
								task.getName(),
								success ? "success" : "failure").put(Duration.ofMillis(executionLength));

						/*try
						{
							ThreadReporter.setReportTime(System.currentTimeMillis());
							ThreadReporter.clearTags();
							ThreadReporter.addTag("host", hostName);
							ThreadReporter.addTag("rollup", rollup.getSaveAs());
							ThreadReporter.addTag("rollup-task", task.getName());
							ThreadReporter.addTag("status", success ? "success" : "failure");
							ThreadReporter.addDataPoint(ROLLUP_TIME, System.currentTimeMillis() - ThreadReporter.getReportTime());
							ThreadReporter.submitData(longDataPointFactory, stringDataPointFactory, publisher);
						}
						catch (DatastoreException e)
						{
							log.error("Could not report metrics for rollup job.", e);
						}*/

						try {
							statusStore.write(task.getId(), status);
						}
						catch (RollUpException e) {
							log.error("Could not write status to status store" , e);
						}
					}
				}
			}
		}
		catch (Throwable t)
		{
			log.error("Failed to execute job " + jobExecutionContext.toString(), t);
		}
	}

	private boolean isJobAlreadyRunning(JobExecutionContext jobExecutionContext, String taskName) throws SchedulerException
	{
		List<JobExecutionContext> jobs = jobExecutionContext.getScheduler().getCurrentlyExecutingJobs();
		for (JobExecutionContext job : jobs) {
			if (job.getTrigger().equals(jobExecutionContext.getTrigger()) && !job.getJobInstance().equals(this)) {
				log.info("There's another instance of task " + taskName + " running so exiting.");
				return true;
			}
		}
		return false;
	}

	@Override
	public void interrupt()
	{
		interrupted = true;
	}
}
