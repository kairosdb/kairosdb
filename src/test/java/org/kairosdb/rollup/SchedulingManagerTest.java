package org.kairosdb.rollup;

import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.core.scheduler.KairosDBScheduler;
import org.kairosdb.eventbus.FilterEventBus;
import org.mockito.Mock;
import org.quartz.impl.JobDetailImpl;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SchedulingManagerTest extends RollupTestBase
{
	private static final String SERVER_GUID = "12345";

	@Mock
	private KairosDatastore mockDatastore;
	@Mock
	private	KairosDBScheduler mockScheduler;
	@Mock
	private	FilterEventBus mockEventBus;
	@Mock
	private RollupTaskStatusStore mockStatusStore;

	private SchedulingManager manager;

	@Before
	public void setup() throws RollUpException
	{
		manager = new SchedulingManager(taskStore, assignmentStore, mockScheduler, mockDatastore, mockExecutionService, mockEventBus, mockStatusStore,10, LOCAL_HOST, SERVER_GUID);
	}

	@Test
	public void testConstructor_taskStore_null_invalid() throws RollUpException
	{
		expectedException.expect(NullPointerException.class);
		expectedException.expectMessage("taskStore cannot be null");

		new SchedulingManager(null, assignmentStore, mockScheduler, mockDatastore, mockExecutionService, mockEventBus, mockStatusStore,10, LOCAL_HOST,SERVER_GUID);
	}

	@Test
	public void testConstructor_assignmentStore_null_invalid() throws RollUpException
	{
		expectedException.expect(NullPointerException.class);
		expectedException.expectMessage("assignmentStore cannot be null");

		new SchedulingManager(taskStore, null, mockScheduler, mockDatastore, mockExecutionService, mockEventBus,mockStatusStore,10, "hostname", SERVER_GUID);
	}

	@Test
	public void testConstructor_scheduler_null_invalid() throws RollUpException
	{
		expectedException.expect(NullPointerException.class);
		expectedException.expectMessage("scheduler cannot be null");

		new SchedulingManager(taskStore, assignmentStore, null, mockDatastore, mockExecutionService, mockEventBus, mockStatusStore,10, "hostname", SERVER_GUID);
	}

	@Test
	public void testConstructor_dataStore_null_invalid() throws RollUpException
	{
		expectedException.expect(NullPointerException.class);
		expectedException.expectMessage("dataStore cannot be null");

		new SchedulingManager(taskStore, assignmentStore, mockScheduler, null, mockExecutionService, mockEventBus, mockStatusStore,10, "hostname", SERVER_GUID);
	}

	@Test
	public void testConstructor_executor_null_invalid() throws RollUpException
	{
		expectedException.expect(NullPointerException.class);
		expectedException.expectMessage("executorService cannot be null");

		new SchedulingManager(taskStore, assignmentStore, mockScheduler, mockDatastore, null, mockEventBus, mockStatusStore,10, "hostname", SERVER_GUID);
	}

	@Test
	public void testConstructor_hostname_null_invalid() throws RollUpException
	{
		expectedException.expect(NullPointerException.class);
		expectedException.expectMessage("hostname cannot be null or empty");

		new SchedulingManager(taskStore, assignmentStore, mockScheduler, mockDatastore, mockExecutionService, mockEventBus,mockStatusStore,10, null, SERVER_GUID);
	}

	@Test
	public void testConstructor_hostname_empty_invalid() throws RollUpException
	{
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("hostname cannot be null or empty");

		new SchedulingManager(taskStore, assignmentStore, mockScheduler, mockDatastore, mockExecutionService, mockEventBus,mockStatusStore,10, "", SERVER_GUID);
	}

	@Test
	public void testConstructor_eventBus_null_invalid() throws RollUpException
	{
		expectedException.expect(NullPointerException.class);
		expectedException.expectMessage("eventBus cannot be null");

		new SchedulingManager(taskStore, assignmentStore, mockScheduler, mockDatastore, mockExecutionService, null, mockStatusStore,10, "hostname", SERVER_GUID);
	}

	@Test
	public void testConstructor_scheduleNewTasks() throws KairosDBException
	{
		addTasks(TASK1, TASK2, TASK3);
		assignmentStore.setAssignment(TASK1.getId(), SERVER_GUID);
		assignmentStore.setAssignment(TASK2.getId(), SERVER_GUID);
		assignmentStore.setAssignment(TASK3.getId(), SERVER_GUID);

		manager.checkSchedulingChanges();

		verify(mockScheduler, times(1)).schedule(SchedulingManager.createJobDetail(TASK1, mockDatastore, LOCAL_HOST, mockEventBus, mockStatusStore), SchedulingManager.createTrigger(TASK1));
		verify(mockScheduler, times(1)).schedule(SchedulingManager.createJobDetail(TASK2, mockDatastore, LOCAL_HOST, mockEventBus, mockStatusStore), SchedulingManager.createTrigger(TASK2));
		verify(mockScheduler, times(1)).schedule(SchedulingManager.createJobDetail(TASK3, mockDatastore, LOCAL_HOST, mockEventBus, mockStatusStore), SchedulingManager.createTrigger(TASK3));
	}

	@Test
	public void testModifiedTasks() throws KairosDBException
	{
		assignmentStore.setAssignment(TASK1.getId(), SERVER_GUID);
		assignmentStore.setAssignment(TASK2.getId(), SERVER_GUID);
		assignmentStore.setAssignment(TASK3.getId(), SERVER_GUID);
		addTasks(TASK1, TASK2, TASK3);

		manager.checkSchedulingChanges();

		// modify task
		RollupTask modifiedTask = new RollupTask(TASK2.getId(), TASK2.getName(), TASK2.getExecutionInterval(), TASK2.getRollups(), "{\"id\": " + TASK2.getId() + ",\"name\": \"" + TASK2.getName() + "\", \"execution_interval\": {\"value\": 1, \"unit\": \"hours\"}}");
		modifiedTask.setLastModified(System.currentTimeMillis() + 10);
		removeTasks(TASK2);
		addTasks(modifiedTask);

		manager.checkSchedulingChanges();

		JobDetailImpl job = SchedulingManager.createJobDetail(TASK2, mockDatastore, LOCAL_HOST, mockEventBus, mockStatusStore);
		verify(mockScheduler, times(1)).cancel(job.getKey());
		verify(mockScheduler, times(2)).schedule(SchedulingManager.createJobDetail(TASK2, mockDatastore, LOCAL_HOST, mockEventBus, mockStatusStore), SchedulingManager.createTrigger(TASK2));
	}

	@Test
	public void testUnscheduleRemovedTasks() throws KairosDBException
	{
		assignmentStore.setAssignment(TASK1.getId(), SERVER_GUID);
		assignmentStore.setAssignment(TASK2.getId(), SERVER_GUID);
		assignmentStore.setAssignment(TASK3.getId(), SERVER_GUID);
		addTasks(TASK1, TASK2, TASK3);

		manager.checkSchedulingChanges();

		// Remove task
		removeTasks(TASK2);

		manager.checkSchedulingChanges();

		JobDetailImpl job = SchedulingManager.createJobDetail(TASK2, mockDatastore, LOCAL_HOST, mockEventBus, mockStatusStore);
		verify(mockScheduler, times(1)).cancel(job.getKey());
		verify(mockScheduler, times(1)).schedule(SchedulingManager.createJobDetail(TASK2, mockDatastore, LOCAL_HOST, mockEventBus, mockStatusStore), SchedulingManager.createTrigger(TASK2));
	}

	@Test
	public void testUnschedulUnassignedTasks() throws KairosDBException
	{
		assignmentStore.setAssignment(TASK1.getId(), SERVER_GUID);
		assignmentStore.setAssignment(TASK2.getId(), SERVER_GUID);
		assignmentStore.setAssignment(TASK3.getId(), SERVER_GUID);
		addTasks(TASK1, TASK2, TASK3);

		manager.checkSchedulingChanges();

		// Remove assignment task
		assignmentStore.removeAssignments(ImmutableSet.of(TASK2.getId()));

		manager.checkSchedulingChanges();

		JobDetailImpl job = SchedulingManager.createJobDetail(TASK2, mockDatastore, LOCAL_HOST, mockEventBus, mockStatusStore);
		verify(mockScheduler, times(1)).cancel(job.getKey());
		verify(mockScheduler, times(1)).schedule(SchedulingManager.createJobDetail(TASK2, mockDatastore, LOCAL_HOST, mockEventBus, mockStatusStore), SchedulingManager.createTrigger(TASK2));
	}
}