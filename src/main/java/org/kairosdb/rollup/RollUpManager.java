package org.kairosdb.rollup;


import com.google.inject.name.Named;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.core.scheduler.KairosDBJob;
import org.kairosdb.core.scheduler.KairosDBScheduler;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;

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

	private long tasksLastModified;

	@Inject
	public RollUpManager(
			@Named(SCHEDULE) String schedule,
			RollUpTasksStore taskStore, KairosDBScheduler scheduler)
	{
		this.schedule = checkNotNullOrEmpty(schedule);
		this.taskStore = checkNotNull(taskStore);
		this.scheduler = checkNotNull(scheduler);
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
				List<RollUpTask> updatedTasks = taskStore.read();

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

	private void updateScheduledTasks(List<RollUpTask> updatedTasks)
	{
		for (RollUpTask task : updatedTasks)
		{
			Long timestamp = taskIdToTimeMap.get(task.getId());
			if (timestamp != null && task.getTimestamp() > timestamp)
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
					logger.info("Updating schedule for rollup " + task.getMetricName());
					scheduler.schedule(new RollUpJob(task));
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

	private void unscheduledRemovedTasks(List<RollUpTask> tasks)
	{
		// todo more elegant way to do this
		Iterator<String> iterator = taskIdToTimeMap.keySet().iterator();
		while(iterator.hasNext())
		{
			String id = iterator.next();

			RollUpTask foundTask = null;
			for (RollUpTask task : tasks)
			{
				if (task.getId().equals(id))
				{
					foundTask = task;
					break;
				}
			}

			if (foundTask != null)
			{
				try
				{
					logger.info("Scheduling rollup " + foundTask.getMetricName());
					iterator.remove();
					scheduler.cancel(id);
				}
				catch (KairosDBException e)
				{
					logger.error("Could not cancel roll up task job " + foundTask, e);
				}
			}
		}
	}

	private void scheduleNewTasks(List<RollUpTask> tasks)
	{
		for (RollUpTask task : tasks)
		{
			if (!taskIdToTimeMap.containsKey(task.getId()))
			{
				try
				{
					logger.info("Scheduling rollup " + task.getMetricName());
					scheduler.schedule("Rollup:" + task.getId(), new RollUpJob(task));
					taskIdToTimeMap.put(task.getId(), task.getTimestamp());
				}
				catch (KairosDBException e)
				{
					logger.error("Failed to schedule new roll up task job " + task, e);
				}
			}
		}
	}
}
