package org.kairosdb.rollup;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.core.scheduler.KairosDBScheduler;
import org.kairosdb.eventbus.FilterEventBus;
import org.mockito.Mock;
import org.quartz.impl.JobDetailImpl;

import java.util.Map;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SchedulingManagerTest extends RollupTestBase
{
	@Mock
	private KairosDatastore mockDatastore;
	@Mock
	private
	KairosDBScheduler mockScheduler;
	@Mock
	private
	FilterEventBus mockEventBus;

	private SchedulingManager manager;

	@Before
	public void setup() throws RollUpException
	{
		manager = new SchedulingManager(taskStore, assignmentStore, mockScheduler, mockDatastore, mockExecutionService, mockEventBus,10, LOCAL_HOST);
	}

	@Test
	public void testConstructor_taskStore_null_invalid() throws RollUpException
	{
		expectedException.expect(NullPointerException.class);
		expectedException.expectMessage("taskStore cannot be null");

		new SchedulingManager(null, assignmentStore, mockScheduler, mockDatastore, mockExecutionService, mockEventBus,10, "hostname");
	}

	@Test
	public void testConstructor_assignmentStore_null_invalid() throws RollUpException
	{
		expectedException.expect(NullPointerException.class);
		expectedException.expectMessage("assignmentStore cannot be null");

		new SchedulingManager(taskStore, null, mockScheduler, mockDatastore, mockExecutionService, mockEventBus,10, "hostname");
	}

	@Test
	public void testConstructor_scheduler_null_invalid() throws RollUpException
	{
		expectedException.expect(NullPointerException.class);
		expectedException.expectMessage("scheduler cannot be null");

		new SchedulingManager(taskStore, assignmentStore, null, mockDatastore, mockExecutionService, mockEventBus,10, "hostname");
	}

	@Test
	public void testConstructor_dataStore_null_invalid() throws RollUpException
	{
		expectedException.expect(NullPointerException.class);
		expectedException.expectMessage("dataStore cannot be null");

		new SchedulingManager(taskStore, assignmentStore, mockScheduler, null, mockExecutionService, mockEventBus,10, "hostname");
	}

	@Test
	public void testConstructor_executor_null_invalid() throws RollUpException
	{
		expectedException.expect(NullPointerException.class);
		expectedException.expectMessage("executorService cannot be null");

		new SchedulingManager(taskStore, assignmentStore, mockScheduler, mockDatastore, null, mockEventBus,10, "hostname");
	}

	@Test
	public void testConstructor_hostname_null_invalid() throws RollUpException
	{
		expectedException.expect(NullPointerException.class);
		expectedException.expectMessage("hostname cannot be null or empty");

		new SchedulingManager(taskStore, assignmentStore, mockScheduler, mockDatastore, mockExecutionService, mockEventBus,10, null);
	}

	@Test
	public void testConstructor_hostname_empty_invalid() throws RollUpException
	{
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("hostname cannot be null or empty");

		new SchedulingManager(taskStore, assignmentStore, mockScheduler, mockDatastore, mockExecutionService, mockEventBus,10, "");
	}

	@Test
	public void testConstructor_eventBus_null_invalid() throws RollUpException
	{
		expectedException.expect(NullPointerException.class);
		expectedException.expectMessage("eventBus cannot be null");

		new SchedulingManager(taskStore, assignmentStore, mockScheduler, mockDatastore, mockExecutionService, null,10, "hostname");
	}

	@Test
	public void testConstructor_scheduleNewTasks() throws KairosDBException
	{
		addTasks(TASK1, TASK2, TASK3);
		assignmentStore.setAssignment(TASK1.getId(), LOCAL_HOST);
		assignmentStore.setAssignment(TASK2.getId(), LOCAL_HOST);
		assignmentStore.setAssignment(TASK3.getId(), LOCAL_HOST);

		manager.checkSchedulingChanges();

		verify(mockScheduler, times(1)).schedule(SchedulingManager.createJobDetail(TASK1, mockDatastore, "localhost", mockEventBus), SchedulingManager.createTrigger(TASK1));
		verify(mockScheduler, times(1)).schedule(SchedulingManager.createJobDetail(TASK2, mockDatastore, "localhost", mockEventBus), SchedulingManager.createTrigger(TASK2));
		verify(mockScheduler, times(1)).schedule(SchedulingManager.createJobDetail(TASK3, mockDatastore, "localhost", mockEventBus), SchedulingManager.createTrigger(TASK3));
	}

	@Test
	public void testModifiedTasks() throws KairosDBException
	{
		assignmentStore.setAssignment(TASK1.getId(), LOCAL_HOST);
		assignmentStore.setAssignment(TASK2.getId(), LOCAL_HOST);
		assignmentStore.setAssignment(TASK3.getId(), LOCAL_HOST);
		addTasks(TASK1, TASK2, TASK3);

		manager.checkSchedulingChanges();

		// modify task
		RollupTask modifiedTask = new RollupTask(TASK2.getId(), TASK2.getName(), TASK2.getExecutionInterval(), TASK2.getRollups(), "{\"id\": " + TASK2.getId() + ",\"name\": \"" + TASK2.getName() + "\", \"execution_interval\": {\"value\": 1, \"unit\": \"hours\"}}");
		modifiedTask.setLastModified(System.currentTimeMillis() + 10);
		removeTasks(TASK2);
		addTasks(modifiedTask);

		manager.checkSchedulingChanges();

		JobDetailImpl job = SchedulingManager.createJobDetail(TASK2, mockDatastore, "localhost", mockEventBus);
		verify(mockScheduler, times(1)).cancel(job.getKey());
		verify(mockScheduler, times(2)).schedule(SchedulingManager.createJobDetail(TASK2, mockDatastore, "localhost", mockEventBus), SchedulingManager.createTrigger(TASK2));
	}

	@Test
	public void testUnscheduleRemovedTasks() throws KairosDBException
	{
		assignmentStore.setAssignment(TASK1.getId(), LOCAL_HOST);
		assignmentStore.setAssignment(TASK2.getId(), LOCAL_HOST);
		assignmentStore.setAssignment(TASK3.getId(), LOCAL_HOST);
		addTasks(TASK1, TASK2, TASK3);

		manager.checkSchedulingChanges();

		// Remove task
		removeTasks(TASK2);

		manager.checkSchedulingChanges();

		JobDetailImpl job = SchedulingManager.createJobDetail(TASK2, mockDatastore, "localhost", mockEventBus);
		verify(mockScheduler, times(1)).cancel(job.getKey());
		verify(mockScheduler, times(1)).schedule(SchedulingManager.createJobDetail(TASK2, mockDatastore, "localhost", mockEventBus), SchedulingManager.createTrigger(TASK2));
	}

	@Test
	public void testUnschedulUnassignedTasks() throws KairosDBException
	{
		assignmentStore.setAssignment(TASK1.getId(), LOCAL_HOST);
		assignmentStore.setAssignment(TASK2.getId(), LOCAL_HOST);
		assignmentStore.setAssignment(TASK3.getId(), LOCAL_HOST);
		addTasks(TASK1, TASK2, TASK3);

		manager.checkSchedulingChanges();

		// Remove assignment task
		assignmentStore.removeAssignments(ImmutableSet.of(TASK2.getId()));

		manager.checkSchedulingChanges();

		JobDetailImpl job = SchedulingManager.createJobDetail(TASK2, mockDatastore, "localhost", mockEventBus);
		verify(mockScheduler, times(1)).cancel(job.getKey());
		verify(mockScheduler, times(1)).schedule(SchedulingManager.createJobDetail(TASK2, mockDatastore, "localhost", mockEventBus), SchedulingManager.createTrigger(TASK2));
	}
}