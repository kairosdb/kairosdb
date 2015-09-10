package org.kairosdb.rollup;


import com.google.inject.name.Named;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.core.scheduler.KairosDBJob;
import org.kairosdb.core.scheduler.KairosDBScheduler;
import org.quartz.*;
import org.quartz.impl.JobDetailImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;
import static org.quartz.TriggerBuilder.newTrigger;

// todo need test
public class RollUpManager implements KairosDBJob
{
	public static final Logger logger = LoggerFactory.getLogger(RollUpManager.class);
	public static final String SCHEDULE = "kairosdb.rollup.rollup_manager_schedule";

	private final String schedule;
	private final RollUpTasksStore taskStore;
	private final KairosDBScheduler scheduler;
	private final Map<String, Long> taskIdToTimeMap = new HashMap<String, Long>();
	private final KairosDatastore dataStore;

	private long tasksLastModified;

	@Inject
	public RollUpManager(
			@Named(SCHEDULE) String schedule,
			RollUpTasksStore taskStore, KairosDBScheduler scheduler, KairosDatastore dataStore)
	{
		this.schedule = checkNotNullOrEmpty(schedule);
		this.taskStore = checkNotNull(taskStore);
		this.scheduler = checkNotNull(scheduler);
		this.dataStore = checkNotNull(dataStore);
	}

	@Override
	public Trigger getTrigger()
	{
		return newTrigger()
				.withIdentity(this.getClass().getSimpleName())
				.withSchedule(CronScheduleBuilder.cronSchedule(schedule))
				.build();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException
	{
		//		logger.info("Rollup manager running");
		try
		{
			long lastModified = taskStore.lastModifiedTime();
			if (lastModified > tasksLastModified)
			{
				logger.info("Reading rollup task config");
				List<RollupTask> updatedTasks = taskStore.read();

				scheduleNewTasks(updatedTasks);
				unscheduledRemovedTasks(updatedTasks);
				updateScheduledTasks(updatedTasks);

				tasksLastModified = lastModified;
			}
		}
		catch (RollUpException e)
		{
			logger.error("Roll up manager failure.", e);
		}
	}

	@Override
	public void interrupt()
	{
		// todo?
	}

	private void updateScheduledTasks(List<RollupTask> updatedTasks)
	{
		for (RollupTask task : updatedTasks)
		{
			Long timestamp = taskIdToTimeMap.get(task.getId());
			if (neverScheduledOrChanged(task, timestamp))
			{
				try
				{
					scheduler.cancel(task.getId());
				}
				catch (KairosDBException e)
				{
					logger.error("Could not cancel roll up task job " + task, e);
					continue;
				}

				try
				{
					logger.info("Updating schedule for rollup " + task.getName());
					JobDetailImpl jobDetail = createJobDetail(task, dataStore);
					scheduler.schedule(jobDetail, createTrigger(task));
				}
				catch (KairosDBException e)
				{
					logger.error("Could not schedule roll up task job " + task, e);
					continue;
				}

				taskIdToTimeMap.put(task.getId(), task.getTimestamp());
			}
		}
	}

	private boolean neverScheduledOrChanged(RollupTask task, Long timestamp)
	{
		return timestamp != null && task.getTimestamp() > timestamp;
	}

	private void unscheduledRemovedTasks(List<RollupTask> tasks)
	{
		// todo more elegant way to do this
		Iterator<String> iterator = taskIdToTimeMap.keySet().iterator();
		while (iterator.hasNext())
		{
			String id = iterator.next();

			RollupTask currentTask = null;
			RollupTask foundTask = null;
			for (RollupTask task : tasks)
			{
				currentTask = task;
				if (task.getId().equals(id))
				{
					foundTask = task;
					break;
				}
			}

			if (foundTask == null)
			{
				try
				{
					logger.info("Cancelling rollup " + currentTask.getName());
					iterator.remove();
					scheduler.cancel(id);
				}
				catch (KairosDBException e)
				{
					logger.error("Could not cancel roll up task job " + currentTask.getName(), e);
				}
			}
		}
	}

	private void scheduleNewTasks(List<RollupTask> tasks)
	{
		for (RollupTask task : tasks)
		{
			if (!taskIdToTimeMap.containsKey(task.getId()))
			{
				try
				{
					logger.info("Scheduling rollup " + task.getName());
					scheduler.schedule(createJobDetail(task, dataStore), createTrigger(task));
					taskIdToTimeMap.put(task.getId(), task.getTimestamp());
				}
				catch (KairosDBException e)
				{
					logger.error("Failed to schedule new roll up task job " + task, e);
				}
			}
		}
	}

	private static JobDetailImpl createJobDetail(RollupTask task, KairosDatastore dataStore)
	{
		JobDetailImpl jobDetail = new JobDetailImpl();
		jobDetail.setJobClass(RollUpJob.class);
		jobDetail.setKey(new JobKey(task.getId() + "-" + RollUpJob.class.getSimpleName()));

		JobDataMap map = new JobDataMap();
		map.put("task", task);
		map.put("datastore", dataStore);
		jobDetail.setJobDataMap(map);
		return jobDetail;
	}

	private static Trigger createTrigger(RollupTask task)
	{
		return newTrigger()
				.withIdentity(task.getId() + "-" + task.getClass().getSimpleName())
				.withSchedule(CronScheduleBuilder.cronSchedule(task.getSchedule()))
				.build();
	}
}
