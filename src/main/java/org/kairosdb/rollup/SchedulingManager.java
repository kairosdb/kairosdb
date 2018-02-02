package org.kairosdb.rollup;


import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.name.Named;
import org.kairosdb.core.KairosDBService;
import org.kairosdb.core.Main;
import org.kairosdb.core.datastore.Duration;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.datastore.TimeUnit;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.core.scheduler.KairosDBScheduler;
import org.kairosdb.eventbus.FilterEventBus;
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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;
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


    private final String hostName;
    private final String serverGuid;
    private final KairosDBScheduler scheduler;
    private final KairosDatastore dataStore;
    private final RollUpAssignmentStore assignmentStore;
    private final RollUpTasksStore taskStore;
    private final ScheduledExecutorService executorService;
    private final FilterEventBus eventBus;
    private final ReentrantLock lock = new ReentrantLock();
    private final RollupTaskStatusStore statusStore;

    private long assignmentsLastModified;
    private long rollupsLastModified;
    private Set<String> assignmentsCache = new HashSet<>();
    private Map<String, RollupTask> tasksCache = new HashMap<>();

    @Inject
    public SchedulingManager(RollUpTasksStore taskStore, RollUpAssignmentStore assignmentStore,
            KairosDBScheduler scheduler, KairosDatastore dataStore,
            @Named(RollUpModule.ROLLUP_EXECUTOR)ScheduledExecutorService executorService,
            FilterEventBus eventBus,
            RollupTaskStatusStore statusStore,
            @Named(DELAY) long delay,
            @Named("HOSTNAME") String hostName,
            @Named(Main.KAIROSDB_SERVER_GUID) String guid)
            throws RollUpException
    {
        this.taskStore = checkNotNull(taskStore, "taskStore cannot be null");
        this.scheduler = checkNotNull(scheduler, "scheduler cannot be null");
        this.dataStore = checkNotNull(dataStore, "dataStore cannot be null");
        this.assignmentStore = checkNotNull(assignmentStore, "assignmentStore cannot be null");
        this.hostName = checkNotNullOrEmpty(hostName, "hostname cannot be null or empty");
        this.executorService = checkNotNull(executorService, "executorService cannot be null or empty");
        this.eventBus = checkNotNull(eventBus, "eventBus cannot be null");
        this.statusStore = checkNotNull(statusStore, "statusStore cannot be null");
        this.serverGuid = checkNotNullOrEmpty(guid, "guid cannot be null or empty");

        // Start thread that checks for rollup changes and rollup assignments
        executorService.scheduleWithFixedDelay(new CheckChanges(), 0, delay, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    private class CheckChanges implements Runnable
    {
        @Override
        public void run()
        {
            checkSchedulingChanges();
        }
    }

    @VisibleForTesting
    void checkSchedulingChanges()
    {
        try {
            long assignmentTime = assignmentStore.getLastModifiedTime();
            long taskStoreTime = taskStore.getLastModifiedTime();
            if (haveRollupsOrAssignmentsChanged(assignmentTime, taskStoreTime)) {
                logger.debug("Checking for roll-up scheduling changes...");

                Set<String> currentAssignments = getAssignmentsCache();
                Map<String, RollupTask> tasks = taskStore.read();
                Set<String> myAssignmentIds = getMyAssignmentIds(serverGuid, assignmentStore.getAssignments());

                // Schedule modified tasks
                rescheduleModifiedTasks(tasks);                                                         // unschedule and then reschedule changed tasks
                removeScheduledTasks(
                        Sets.union(Sets.difference(currentAssignments, tasks.keySet()),                 // unschedule removed tasks or tasks not longer assigned to me
                                Sets.difference(currentAssignments, myAssignmentIds)));

                // Schedule new assignments
                scheduleNewTasks(Sets.difference(myAssignmentIds, currentAssignments));                 // schedule newly assigned tasks

                // Update caches
                lock.lock();
                try {
                    assignmentsCache = myAssignmentIds;
                    assignmentsLastModified = assignmentTime;
                    rollupsLastModified = taskStoreTime;
                    tasksCache = tasks;
                }
                finally {
                    lock.unlock();
                }
            }
        }
        catch (Throwable e) {
            logger.error("Failed to modify roll-up scheduling", e);
        }
    }

    private Set<String> getAssignmentsCache()
    {
        lock.lock();
        try
        {
            return ImmutableSet.copyOf(assignmentsCache);
        }
        finally {
            lock.unlock();
        }
    }

    private Set<String> getMyAssignmentIds(String guid, Map<String, String> assignments)
    {
        Set<String> myIds = new HashSet<>();
        for (String id : assignments.keySet()) {
            if (assignments.get(id).equals(guid))
            {
                myIds.add(id);
            }
        }
        return myIds;
    }

    private boolean haveRollupsOrAssignmentsChanged(long assignmentTime, long taskStoreTime)
            throws RollUpException
    {
        lock.lock();
        try {
            return assignmentsLastModified == 0 ||
                    rollupsLastModified == 0 ||
                    assignmentsLastModified != assignmentTime ||
                    rollupsLastModified != taskStoreTime;
        }
        finally {
            lock.unlock();
        }
    }

    private void rescheduleModifiedTasks(Map<String, RollupTask> tasks)
    {
        for (String id : tasks.keySet()) {
            RollupTask existingTask = tasksCache.get(id);
            if (existingTask != null && tasks.get(id).getLastModified() != existingTask.getLastModified()) {
                updateScheduledTask(tasks.get(id));
            }
        }
    }

    private void scheduleNewTasks(Set<String> ids)
    {
        for (String id : ids) {
            try {
                RollupTask task = taskStore.read(id);
                if (task != null) {
                    Trigger trigger = createTrigger(task);
                    JobDetailImpl jobDetail = createJobDetail(task, dataStore, hostName, eventBus, statusStore);
                    scheduler.schedule(jobDetail, trigger);
                    updateStatus(task, trigger.getNextFireTime());
                    logger.info("Scheduled roll-up task " + task.getName() + " with id " + jobDetail.getFullName() + ". Next execution time " + trigger.getNextFireTime());
                }
                else {
                    logger.error("A roll-up task does not exist for id: " + id);
                }
            }
            catch (RollUpException e) {
                logger.error("Could not read task for id " + id, e);
            }
            catch (KairosDBException e) {
                logger.error("Failed to schedule new roll up task job " + id, e);
            }
        }
    }

    private void updateScheduledTask(RollupTask task)
    {
        try {
            scheduler.cancel(getJobKey(task.getId()));
        }
        catch (KairosDBException e) {
            logger.error("Could not cancel roll up task job " + task, e);
            return;
        }

        try {
            logger.info("Updating schedule for rollup " + task.getName());
            JobDetailImpl jobDetail = createJobDetail(task, dataStore, hostName, eventBus, statusStore);
            Trigger trigger = createTrigger(task);
            scheduler.schedule(jobDetail, trigger);
            logger.info("Roll-up task " + task.getName() + " with id " + jobDetail.getKey() + " scheduled. Next execution time " + trigger.getNextFireTime());
        }
        catch (KairosDBException e) {
            logger.error("Could not schedule roll up task job " + task, e);
        }
    }

    private void removeScheduledTasks(Set<String> ids)
    {
        for (String id : ids) {
            try {
                JobKey jobKey = getJobKey(id);
                logger.info("Cancelling rollup " + id);
                scheduler.cancel(jobKey);
            }
            catch (RollUpException e) {
                logger.error("Could not read task for id " + id, e);
            }
            catch (KairosDBException e) {
                logger.error("Could not cancel roll up task job " + id, e);
            }
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
            statusStore.write(task.getId(), status);
        }
        catch (RollUpException e) {
            logger.error("Could not update status.", e);
        }
    }

    private RollupTaskStatus getOrCreateStatus(RollupTask task, Date nextExecutionTime)
            throws RollUpException
    {
        RollupTaskStatus status = statusStore.read(task.getId());
        if (status == null) {
            return new RollupTaskStatus(nextExecutionTime, hostName);
        }
        status.setNextScheduled(nextExecutionTime);
        return status;
    }

    private static JobKey getJobKey(String id)
    {
        return new JobKey(id, GROUP_ID);
    }

    @VisibleForTesting
    static JobDetailImpl createJobDetail(RollupTask task, KairosDatastore dataStore, String hostName, FilterEventBus eventBus, RollupTaskStatusStore statusStore)
    {
        JobDetailImpl jobDetail = new JobDetailImpl();
        jobDetail.setJobClass(RollUpJob.class);
        jobDetail.setKey(getJobKey(task.getId()));

        JobDataMap map = new JobDataMap();
        map.put("task", task);
        map.put("datastore", dataStore);
        map.put("hostName", hostName);
        map.put("eventBus", eventBus);
        map.put("statusStore", statusStore);
        jobDetail.setJobDataMap(map);
        return jobDetail;
    }

    @VisibleForTesting
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
    public void start()
            throws KairosDBException
    {
    }

    @Override
    public void stop()
    {
        executorService.shutdown();
    }
}
