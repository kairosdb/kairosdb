package org.kairosdb.rollup;

import com.google.inject.name.Named;
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
import javax.inject.Inject;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public class RollUpJob implements InterruptableJob
{
	private static final Logger log = LoggerFactory.getLogger(KairosDBSchedulerImpl.class);
	private static final RollupStats stats = MetricSourceManager.getSource(RollupStats.class);

	private static final String ROLLUP_TIME = "kairosdb.rollup.execution-time";
	private final KairosDatastore m_datastore;
	private final Publisher<DataPointEvent> m_publisher;
	private final String m_hostName;
	private final RollupTaskStatusStore m_statusStore;

	private boolean interrupted;
	private LongDataPointFactory longDataPointFactory = new LongDataPointFactoryImpl();
	private StringDataPointFactory stringDataPointFactory = new StringDataPointFactory();

	@Inject
	public RollUpJob(KairosDatastore datastore, FilterEventBus eventBus,
			@Named("HOSTNAME") String hostName,
			RollupTaskStatusStore statusStore)
	{
		//This class is a singleton, all jobs run from this same class
		//instance variables should not hold per job data.
		m_datastore = datastore;
		m_hostName = hostName;
		m_statusStore = statusStore;

		m_publisher = eventBus.createPublisher(DataPointEvent.class);
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
			checkState(task != null, "Task was null");

			if (isJobAlreadyRunning(jobExecutionContext, task.getName())) return;

			for (Rollup rollup : task.getRollups())
			{
				log.info("Executing Rollup Task: " + task.getName() + " for Rollup  " + rollup.getSaveAs());
				stats.jobRunCount(rollup.getSaveAs(), task.getName()).put(1);
				stats.jobQueryCount(rollup.getSaveAs(), task.getName()).put(rollup.getQueryMetrics().size());

				RollupTaskStatus status = new RollupTaskStatus(jobExecutionContext.getNextFireTime(), m_hostName);
				RollupProcessor processor = new RollupProcessorImpl(m_datastore);

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
					long executionLength = 0L;
					try
					{
						long executionStartTime = System.currentTimeMillis();
						long dpCount = processor.process(m_statusStore, task, queryMetric, rollup.getTimeZone());
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

						try {
							m_statusStore.write(task.getId(), status);
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
