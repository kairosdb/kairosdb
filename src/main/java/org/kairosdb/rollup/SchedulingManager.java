package org.kairosdb.rollup;


import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.name.Named;
import org.kairosdb.core.KairosDBService;
import org.kairosdb.core.Main;
import org.kairosdb.core.datastore.Duration;
import org.kairosdb.core.datastore.QueryMetric;
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
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Objects.requireNonNull;
import static com.google.common.base.Preconditions.checkState;
import static org.kairosdb.util.Preconditions.requireNonNullOrEmpty;
import static org.quartz.CalendarIntervalScheduleBuilder.calendarIntervalSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

/**
 * Managers the scheduling of roll-ups.
 */
public class SchedulingManager implements KairosDBService
{
	public static final Logger logger = LoggerFactory.getLogger(SchedulingManager.class);

	private static final String GROUP_ID = RollUpJob.class.getSimpleName();
	private static final String DELAY = "kairosdb.rollups.server_assignment.check_update_delay_millseconds";


	private final String m_hostName;
	private final String m_serverGuid;
	private final KairosDBScheduler m_scheduler;
	private final RollUpAssignmentStore m_assignmentStore;
	private final RollUpTasksStore m_taskStore;
	private final ScheduledExecutorService m_executorService;
	private final RollupTaskStatusStore m_statusStore;
	private final long m_checkDelay;

	private long m_assignmentsLastModified;
	private long m_rollupsLastModified;
	private Set<String> m_cachedAssignments = new HashSet<>();
	private Map<String, RollupTask> m_tasksCache = new HashMap<>();

	@Inject
	public SchedulingManager(RollUpTasksStore taskStore, RollUpAssignmentStore assignmentStore,
			KairosDBScheduler scheduler, @Named(RollUpModule.ROLLUP_EXECUTOR) ScheduledExecutorService executorService,
			RollupTaskStatusStore statusStore, @Named(DELAY) long delay, @Named("HOSTNAME") String hostName,
			@Named(Main.KAIROSDB_SERVER_GUID) String guid) throws RollUpException
	{
		m_taskStore = taskStore;
		m_scheduler = scheduler;
		m_assignmentStore = assignmentStore;
		m_hostName = hostName;
		m_executorService = executorService;
		m_statusStore = statusStore;
		m_serverGuid = guid;
		m_checkDelay = delay;
	}


	@VisibleForTesting
	void checkSchedulingChanges()
	{
		try {
			long assignmentTime = m_assignmentStore.getLastModifiedTime();
			if (m_assignmentsLastModified != assignmentTime) {
				//make sure we have jobs for all assigned tasks
				Set<String> newAssignedTaskIds = m_assignmentStore.getAssignedIds(m_serverGuid);

				Set<String> delTasks = Sets.difference(m_cachedAssignments, newAssignedTaskIds);
				removeScheduledTasks(delTasks);

				Set<String> newTasks = Sets.difference(newAssignedTaskIds, m_cachedAssignments);
				scheduleNewTasks(newTasks);

				m_cachedAssignments = newAssignedTaskIds;
				m_assignmentsLastModified = assignmentTime;
			}


			long taskStoreTime = m_taskStore.getLastModifiedTime();
			if (m_rollupsLastModified != taskStoreTime) {
				rescheduleModifiedTasks();
				m_rollupsLastModified = taskStoreTime;
			}
		}
		catch (Throwable e) {
			logger.error("Failed to modify roll-up scheduling", e);
		}
	}

	private void rescheduleModifiedTasks() throws RollUpException
	{
		for (String assignment : m_cachedAssignments) {
			RollupTask task = m_taskStore.read(assignment);
			if (task != null) { //may have been deleted on us
				RollupTask cachedTask = m_tasksCache.get(assignment);
				if (cachedTask == null || task.getLastModified() != cachedTask.getLastModified())
					updateScheduledTask(task);
			}
			else
			{
				removeScheduledTask(assignment);
			}
		}

		//cleanup cache to only assigned tasks
		m_tasksCache.entrySet().removeIf(entry -> !m_cachedAssignments.contains(entry.getKey()));
	}

	private void scheduleNewTask(String taskId)
	{
		try {
			RollupTask task = m_taskStore.read(taskId);
			if (task != null) {
				Trigger trigger = createTrigger(task);
				JobDetailImpl jobDetail = createJobDetail(task);
				m_scheduler.schedule(jobDetail, trigger);
				updateStatus(task, trigger.getNextFireTime());
				m_tasksCache.put(taskId, task);
				logger.info("Scheduled roll-up task " + task.getName() + " with id " + jobDetail.getFullName() + ". Next execution time " + trigger.getNextFireTime());
			}
			else {
				logger.error("A roll-up task does not exist for id: " + taskId);
			}
		}
		catch (RollUpException e) {
			logger.error("Could not read task for id " + taskId, e);
		}
		catch (KairosDBException e) {
			logger.error("Failed to schedule new roll up task job " + taskId, e);
		}
	}

	private void scheduleNewTasks(Set<String> ids)
	{
		for (String id : ids) {
			scheduleNewTask(id);
		}
	}

	private void updateScheduledTask(RollupTask task)
	{
		removeScheduledTask(task.getId());

		logger.info("Updating schedule for rollup " + task.getName());
		scheduleNewTask(task.getId());
	}

	private void removeScheduledTask(String taskId)
	{
		try {
			JobKey jobKey = getJobKey(taskId);
			logger.info("Cancelling rollup " + taskId);
			m_scheduler.cancel(jobKey);
		}
		catch (RollUpException e) {
			logger.error("Could not read task for id " + taskId, e);
		}
		catch (KairosDBException e) {
			logger.error("Could not cancel roll up task job " + taskId, e);
		}
	}

	private void removeScheduledTasks(Set<String> ids)
	{
		for (String id : ids) {
			removeScheduledTask(id);
		}
	}

	private void updateStatus(RollupTask task, Date nextExecutionTime)
	{
		try {
			RollupTaskStatus status = getOrCreateStatus(task, nextExecutionTime);

			if (status.getStatuses().isEmpty() && !task.getRollups().isEmpty() && !task.getRollups().get(0).getQueryMetrics().isEmpty()) {
				// Add empty status
				QueryMetric metric = task.getRollups().get(0).getQueryMetrics().get(0);
				status.addStatus(RollupTaskStatus.createQueryMetricStatus(metric.getName(), 0, 0, 0));
			}
			m_statusStore.write(task.getId(), status);
		}
		catch (RollUpException e) {
			logger.error("Could not update status.", e);
		}
	}

	private RollupTaskStatus getOrCreateStatus(RollupTask task, Date nextExecutionTime) throws RollUpException
	{
		RollupTaskStatus status = m_statusStore.read(task.getId());
		if (status == null) {
			return new RollupTaskStatus(nextExecutionTime, m_hostName);
		}
		status.setNextScheduled(nextExecutionTime);
		return status;
	}

	private static JobKey getJobKey(String id)
	{
		return new JobKey(id, GROUP_ID);
	}

	@VisibleForTesting
	static JobDetailImpl createJobDetail(RollupTask task)
	{
		JobDetailImpl jobDetail = new JobDetailImpl();
		jobDetail.setJobClass(RollUpJob.class);
		jobDetail.setKey(getJobKey(task.getId()));

		JobDataMap map = new JobDataMap();
		map.put("task", task);
		jobDetail.setJobDataMap(map);
		return jobDetail;
	}

	@VisibleForTesting
	@SuppressWarnings("ConstantConditions")
	static Trigger createTrigger(RollupTask task)
	{
		Duration executionInterval = task.getExecutionInterval();
		return newTrigger().withIdentity(task.getId(), GROUP_ID).startAt(DateBuilder.futureDate((int) executionInterval.getValue(), toIntervalUnit(executionInterval.getUnit()))).withSchedule(calendarIntervalSchedule().withInterval((int) executionInterval.getValue(), toIntervalUnit(executionInterval.getUnit()))).build();
	}

	private static DateBuilder.IntervalUnit toIntervalUnit(TimeUnit unit)
	{
		switch (unit) {
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

	@Override
	public void start() throws KairosDBException
	{
		// Start thread that checks for rollup changes and rollup assignments
		m_executorService.scheduleWithFixedDelay(this::checkSchedulingChanges, 0, m_checkDelay, java.util.concurrent.TimeUnit.MILLISECONDS);
	}

	@Override
	public void stop()
	{
		m_executorService.shutdown();
	}
}
