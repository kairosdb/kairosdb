package org.kairosdb.rollup;


import com.google.inject.name.Named;
import org.kairosdb.core.datastore.Duration;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.datastore.TimeUnit;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.core.scheduler.KairosDBScheduler;
import org.quartz.DateBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobKey;
import org.quartz.Trigger;
import org.quartz.impl.JobDetailImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.quartz.CalendarIntervalScheduleBuilder.calendarIntervalSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

public class RollUpManager implements RollupTaskChangeListener
{
	public static final Logger logger = LoggerFactory.getLogger(RollUpManager.class);
	private static final String GROUP_ID = RollUpJob.class.getSimpleName();

	private final KairosDBScheduler scheduler;
	private final KairosDatastore dataStore;

	@Inject
	@Named("HOSTNAME")
	private String hostName = "localhost";

	@Inject
	public RollUpManager(RollUpTasksStore taskStore,
			KairosDBScheduler scheduler, KairosDatastore dataStore) throws RollUpException
	{
		checkNotNull(taskStore, "taskStore cannot be null");
		this.scheduler = checkNotNull(scheduler, "scheduler cannot be null");
		this.dataStore = checkNotNull(dataStore, "dataStore cannot be null");

		// Load saved tasks
		List<RollupTask> tasks = taskStore.read();
		for (RollupTask task : tasks)
		{
			scheduleNewTask(task);
		}

		taskStore.addListener(this);
	}

	@Override
	public void change(RollupTask task, Action action)
	{
		checkNotNull(task, "task cannot be null");
		switch (action)
		{
			case ADDED:
				scheduleNewTask(task);
				break;
			case CHANGED:
				updateScheduledTask(task);
				break;
			case REMOVED:
				removeScheduledTask(task);
				break;
		}
	}

	private void scheduleNewTask(RollupTask task)
	{
		try
		{
			logger.info("Scheduling rollup " + task.getName());
			Trigger trigger = createTrigger(task);
			JobDetailImpl jobDetail = createJobDetail(task, dataStore, hostName);
			scheduler.schedule(jobDetail, trigger);
			logger.info("Roll-up task " + jobDetail.getFullName() + " scheduled. Next execution time " + trigger.getNextFireTime());
		}
		catch (KairosDBException e)
		{
			logger.error("Failed to schedule new roll up task job " + task, e);
		}
	}

	private void updateScheduledTask(RollupTask task)
	{
		try
		{
			scheduler.cancel(getJobKey(task));
		}
		catch (KairosDBException e)
		{
			logger.error("Could not cancel roll up task job " + task, e);
			return;
		}

		try
		{
			logger.info("Updating schedule for rollup " + task.getName());
			JobDetailImpl jobDetail = createJobDetail(task, dataStore, hostName);
			Trigger trigger = createTrigger(task);
			scheduler.schedule(jobDetail, trigger);
			logger.info("Roll-up task " + jobDetail.getFullName() + " scheduled. Next execution time " + trigger.getNextFireTime());
		}
		catch (KairosDBException e)
		{
			logger.error("Could not schedule roll up task job " + task, e);
		}
	}

	private void removeScheduledTask(RollupTask task)
	{
		try
		{
			JobKey jobKey = getJobKey(task);
			logger.info("Cancelling rollup " + task.getName());
			scheduler.cancel(jobKey);
		}
		catch (KairosDBException e)
		{
			logger.error("Could not cancel roll up task job " + task.getName(), e);
		}
	}

	private static JobKey getJobKey(RollupTask task)
	{
		return new JobKey(task.getId() + "-" + task.getName(), RollUpJob.class.getSimpleName());
	}

	static JobDetailImpl createJobDetail(RollupTask task, KairosDatastore dataStore, String hostName)
	{
		JobDetailImpl jobDetail = new JobDetailImpl();
		jobDetail.setJobClass(RollUpJob.class);
		jobDetail.setKey(getJobKey(task));

		JobDataMap map = new JobDataMap();
		map.put("task", task);
		map.put("datastore", dataStore);
		map.put("hostName", hostName);
		jobDetail.setJobDataMap(map);
		return jobDetail;
	}

	@SuppressWarnings("ConstantConditions")
	static Trigger createTrigger(RollupTask task)
	{
		Duration executionInterval = task.getExecutionInterval();
		return newTrigger()
				.withIdentity(task.getId(), GROUP_ID)
				.startAt(DateBuilder.futureDate((int) executionInterval.getValue(), toIntervalUnit(executionInterval.getUnit())))
				.withSchedule(calendarIntervalSchedule()
						.withInterval((int) executionInterval.getValue(), toIntervalUnit(executionInterval.getUnit())))
				.build();
	}

	private static DateBuilder.IntervalUnit toIntervalUnit(TimeUnit unit)
	{
		switch (unit)
		{
			case MILLISECONDS:
				return DateBuilder.IntervalUnit.MILLISECOND;
			case SECONDS:
				return DateBuilder.IntervalUnit.SECOND;
			case MINUTES:
				return DateBuilder.IntervalUnit.MINUTE;
			case HOURS:
				return DateBuilder.IntervalUnit.HOUR;
			case DAYS:
				return DateBuilder.IntervalUnit.DAY;
			case WEEKS:
				return DateBuilder.IntervalUnit.WEEK;
			case MONTHS:
				return DateBuilder.IntervalUnit.MONTH;
			case YEARS:
				return DateBuilder.IntervalUnit.YEAR;
			default:
				checkState(false, "Invalid time unit" + unit);
				return null;
		}
	}
}
