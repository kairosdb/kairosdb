package org.kairosdb.rollup;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MapDifference;
import com.google.common.collect.MapDifference.ValueDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.HostManager;
import org.kairosdb.core.KairosDBService;
import org.kairosdb.core.Main;
import org.kairosdb.core.datastore.Duration;
import org.kairosdb.core.datastore.ServiceKeyValue;
import org.kairosdb.core.datastore.TimeUnit;
import org.kairosdb.core.exception.KairosDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;

/**
 * Manages Roll-up server assignments. Assignments identify which Kairos host executes what roll-ups.
 * Associates TaskId with Host guid
 */
public class AssignmentManager implements KairosDBService
{
    public static final Logger logger = LoggerFactory.getLogger(AssignmentManager.class);
    private static final String DELAY = "kairosdb.rollups.server_assignment.check_update_delay_millseconds";

    private final RollUpAssignmentStore assignmentStore;
    private final RollUpTasksStore taskStore;
    private final RollupTaskStatusStore statusStore;
    private final ScheduledExecutorService executorService;
    private final BalancingAlgorithm balancing;
    private final HostManager hostManager;
    private final String guid;
    private final ReentrantLock lock = new ReentrantLock();

    private long assignmentsLastModified;
    private long rollupsLastModified;

    private Map<String, String> assignmentsCache = new TreeMap<>();


    @Inject
    public AssignmentManager(@Named(Main.KAIROSDB_SERVER_GUID) String guid,
            RollUpTasksStore taskStore,
            RollUpAssignmentStore assignmentStore,
            RollupTaskStatusStore statusStore,
            @Named(RollUpModule.ROLLUP_EXECUTOR) ScheduledExecutorService executorService, HostManager hostManager,
            BalancingAlgorithm balancing, @Named(DELAY) long delay)
    {
        this.guid = checkNotNullOrEmpty(guid, "guid cannot be null or empty");
        this.assignmentStore = checkNotNull(assignmentStore, "assignmentStore cannot be null");
        this.taskStore = checkNotNull(taskStore, "taskStore cannot be null");
        this.statusStore = checkNotNull(statusStore, "statusStore cannot be null");

        this.executorService = checkNotNull(executorService, "executorService cannot be null");
        this.balancing = checkNotNull(balancing, "balancing cannot be null");
        this.hostManager = checkNotNull(hostManager, "hostManager cannot be null");

        // Start thread that checks for rollup changes and rollup assignments
        executorService.scheduleWithFixedDelay(new updateAssignments(), 0, delay, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    private class updateAssignments implements Runnable
    {
        @Override
        public void run()
        {
            checkAssignmentChanges();
        }
    }

    @VisibleForTesting
    void checkAssignmentChanges()
    {
        try {
            long assignmentTime = assignmentStore.getLastModifiedTime();
            long taskStoreTime = taskStore.getLastModifiedTime();

            if (haveRollupsOrAssignmentsChanged(assignmentTime, taskStoreTime)) {
                Map<String, String> previousAssignments = getAssignmentsCache();
                Map<String, String> assignments = assignmentStore.getAssignments();
                Map<String, String> newAssignments = new HashMap<>(assignments);
                Map<String, RollupTask> tasks = taskStore.read();
                Map<String, ServiceKeyValue> hosts = hostManager.getActiveKairosHosts();

                if (getMyAssignmentIds(guid, newAssignments).isEmpty() && tasks.size() > hosts.size()) {
                    logger.info("Server starting up. Reblanacing roll-up assignments");
                    newAssignments = balancing.rebalance(hosts.keySet(), getScores(tasks));
                }
                else {
                    logger.debug("Checking for roll-up assignment changes...");
                    // Remove assignments for task that have been removed
                    SetView<String> removedTasks = Sets.difference(previousAssignments.keySet(), tasks.keySet());
                    for (String taskToRemove : removedTasks) {
                        newAssignments.remove(taskToRemove);
                        statusStore.remove(taskToRemove);
                    }

                    // Remove assignments for hosts that are inactive
                    Set<String> tasksForHostsRemoved = getTasksForHostsNowInactive(previousAssignments, hosts);
                    for (String assignmentToRemove : tasksForHostsRemoved) {
                        newAssignments.remove(assignmentToRemove);
                    }

                    // Add assignments to unassigned tasks
                    newAssignments.putAll(balancing.balance(hosts.keySet(), newAssignments, getScores(tasks)));
                }

                // Save changes to the assignments table
                saveChangesToAssignmentTable(assignments, newAssignments);

                // Update caches
                lock.lock();
                try {
                    assignmentsCache = newAssignments;
                    assignmentsLastModified = assignmentTime;
                    rollupsLastModified = taskStoreTime;
                }
                finally {
                    lock.unlock();
                }
            }
        }
        catch (Throwable e) {
            logger.error("Failed to modify roll-up assignments", e);
        }
    }

    private void saveChangesToAssignmentTable(Map<String, String> assignments, Map<String, String> newAssignments)
            throws RollUpException
    {
        MapDifference<String, String> diff = Maps.difference(assignments, newAssignments);
        if (!diff.areEqual()) {
            Map<String, String> remove = diff.entriesOnlyOnLeft();
            Map<String, String> add = diff.entriesOnlyOnRight();
            Map<String, ValueDifference<String>> entryDifferences = diff.entriesDiffering();

            if (!remove.isEmpty()) {
                assignmentStore.removeAssignments(remove.keySet());
            }

            for (String id : add.keySet()) {
                assignmentStore.setAssignment(id, add.get(id));
            }

            for (String id : entryDifferences.keySet()) {
                assignmentStore.removeAssignments(ImmutableSet.of(id));
                assignmentStore.setAssignment(id, entryDifferences.get(id).rightValue());
            }
        }
    }

    private static Set<String> getTasksForHostsNowInactive(Map<String, String> previousAssignments, Map<String, ServiceKeyValue> hosts)
    {
        SetView<String> inactiveHosts = Sets.difference(new HashSet<>(previousAssignments.values()), hosts.keySet());

        Set<String> removedTaskIds = new HashSet<>();
        for (Entry<String, String> assignment : previousAssignments.entrySet()) {
            if (inactiveHosts.contains(assignment.getValue())) {
                removedTaskIds.add(assignment.getKey());
            }
        }
        return removedTaskIds;
    }

    private Map<String, String> getAssignmentsCache()
    {
        lock.lock();
        try {
            return ImmutableMap.copyOf(assignmentsCache);
        }
        finally {
            lock.unlock();
        }
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

    private static Set<String> getMyAssignmentIds(String guid, Map<String, String> assignments)
    {
        Set<String> myIds = new HashSet<>();
        for (String host: assignments.values()) {
            if (host.equals(guid)) {
                myIds.add(host);
            }
        }
        return myIds;
    }


    private static Map<String, Long> getScores(Map<String, RollupTask> tasks)
    {
        Map<String, Long> scores = new HashMap<>();
        for (String id : tasks.keySet()) {
            scores.put(id, score(tasks.get(id)));
        }
        return scores;
    }

    /**
     * Returns a score for the task based on the execution interval.
     * Score values are as follows:
     * 1 second -> 120
     * 1 minute -> 60
     * 1 hour -> 1
     * > 1 hour -> 1
     */
    @VisibleForTesting
    static long score(RollupTask task)
    {
        Duration executionInterval = task.getExecutionInterval();
        if (executionInterval.getUnit().ordinal() > 2) {
            // 1 hour or greater
            return 1;
        }
        else if (executionInterval.getUnit().equals(TimeUnit.MINUTES)) {
            return 61 - executionInterval.getValue();
        }
        else if (executionInterval.getUnit().equals(TimeUnit.SECONDS)) {
            return 121 - executionInterval.getValue();
        }
        else {
            //noinspection ConstantConditions
            throw new IllegalArgumentException("Invalid time unit " + executionInterval.getUnit());
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
