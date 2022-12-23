package org.kairosdb.rollup;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.inject.name.Named;
import org.kairosdb.core.HostManager;
import org.kairosdb.core.KairosDBService;
import org.kairosdb.core.datastore.Duration;
import org.kairosdb.core.datastore.ServiceKeyValue;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.eventbus.Subscribe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class AssignmentCoordinator implements KairosDBService
{
	public static final Logger logger = LoggerFactory.getLogger(AssignmentCoordinator.class);
	public static final String DELAY = "kairosdb.rollups.server_assignment.check_update_delay_millseconds";

	private final RollUpTasksStore m_taskStore;
	private final RollUpAssignmentStore m_assignmentStore;
	private final RollupTaskStatusStore m_statusStore;
	private final ScheduledExecutorService m_executorService;
	private final BalancingAlgorithm m_balancingAlgorithm;
	private final long m_delay;
	private final Object m_rebalanceLock = new Object();

	private ScheduledFuture<?> m_coordinatorTaskFuture;
	private Map<String, ServiceKeyValue> m_hostMap = Collections.emptyMap();
	private long m_rollupsLastModified;

	@Inject
	public AssignmentCoordinator(RollUpTasksStore taskStore, RollUpAssignmentStore assignmentStore,
			RollupTaskStatusStore statusStore, @Named(RollUpModule.ROLLUP_EXECUTOR) ScheduledExecutorService executorService,
			BalancingAlgorithm balancing, @Named(DELAY) long delay)
	{
		m_taskStore = taskStore;
		m_assignmentStore = assignmentStore;
		m_statusStore = statusStore;
		m_executorService = executorService;
		m_balancingAlgorithm = balancing;
		m_delay = delay;
	}

	private void rebalanceAssignments() throws RollUpException
	{
		synchronized (m_rebalanceLock) {
			logger.debug("Rebalancing rollup assignments");
			Map<String, String> assignments = m_assignmentStore.getAssignments();
			Map<String, RollupTask> tasks = m_taskStore.read();

			Map<String, String> newAssignments = m_balancingAlgorithm.rebalance(m_hostMap.keySet(), getScores(tasks));

			// Save changes to the assignments table
			saveChangesToAssignmentTable(assignments, newAssignments);
		}
	}

	/*package*/ void checkAssignmentChanges()
	{
		try {
			//check task store for unassigned tasks
			long taskStoreTime = m_taskStore.getLastModifiedTime();
			if (m_rollupsLastModified != taskStoreTime) {
				rebalanceAssignments();
				m_rollupsLastModified = taskStoreTime;
			}
		}
		catch (RollUpException e) {
			logger.error("Unable to rebalance rollup assignments", e);
		}
	}

	@Subscribe
	public void hostListChange(HostManager.HostChangeEvent hostChangeEvent)
	{
		try {
			logger.debug("Host list changed");
			m_hostMap = hostChangeEvent.getHostMap();
			if (m_coordinatorTaskFuture != null) rebalanceAssignments();
		}
		catch (RollUpException e) {
			logger.error("Unable to rebalance rollup assignments", e);
		}
	}

	@Subscribe
	public void coordinatorChanged(HostManager.CoordinatorChangeEvent coordinatorChangeEvent)
	{
		if (coordinatorChangeEvent.isCoordinator())
		{
			if (!m_executorService.isShutdown())
			{
				//turn on task assigner
				logger.debug("We are the rollup coordinator");
				m_coordinatorTaskFuture = m_executorService.scheduleWithFixedDelay(this::checkAssignmentChanges, 0, m_delay, TimeUnit.MILLISECONDS);
			}
		}
		else
		{
			if (m_coordinatorTaskFuture != null)
			{
				//turn it off
				logger.debug("No longer the rollup coordinator");
				m_coordinatorTaskFuture.cancel(false);
				m_coordinatorTaskFuture = null;
			}
		}
	}

	private void saveChangesToAssignmentTable(Map<String, String> assignments, Map<String, String> newAssignments)
			throws RollUpException
	{
		MapDifference<String, String> diff = Maps.difference(assignments, newAssignments);
		if (!diff.areEqual()) {
			Map<String, String> remove = diff.entriesOnlyOnLeft();
			Map<String, String> add = diff.entriesOnlyOnRight();
			Map<String, MapDifference.ValueDifference<String>> entryDifferences = diff.entriesDiffering();

			if (!remove.isEmpty()) {
				m_assignmentStore.removeAssignments(remove.keySet());
			}

			for (String id : add.keySet()) {
				m_assignmentStore.setAssignment(id, add.get(id));
			}

			for (String id : entryDifferences.keySet()) {
				m_assignmentStore.removeAssignments(ImmutableSet.of(id));
				m_assignmentStore.setAssignment(id, entryDifferences.get(id).rightValue());
			}
		}
	}

	@Override
	public void start() throws KairosDBException
	{
		logger.debug("AssignmentCoordinator starting");
	}

	@Override
	public void stop()
	{
		logger.debug("AssignmentCoordinator stopping");
		if (!m_executorService.isShutdown())
			m_executorService.shutdown();
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
		else if (executionInterval.getUnit().equals(org.kairosdb.core.datastore.TimeUnit.MINUTES)) {
			return 61 - executionInterval.getValue();
		}
		else if (executionInterval.getUnit().equals(org.kairosdb.core.datastore.TimeUnit.SECONDS)) {
			return 121 - executionInterval.getValue();
		}
		else {
			throw new IllegalArgumentException("Invalid time unit " + executionInterval.getUnit());
		}
	}
}
