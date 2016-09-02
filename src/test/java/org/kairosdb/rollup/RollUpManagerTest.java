package org.kairosdb.rollup;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.kairosdb.core.datastore.Duration;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.datastore.TimeUnit;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.core.scheduler.KairosDBScheduler;
import org.quartz.impl.JobDetailImpl;

import java.util.ArrayList;
import java.util.List;

import static org.kairosdb.rollup.RollupTaskChangeListener.Action;
import static org.mockito.Mockito.*;

public class RollUpManagerTest
{
	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	private RollUpTasksStore mockTaskStore;
	private KairosDBScheduler mockScheduler;
	private KairosDatastore mockDatastore;

	@Before
	public void setup() throws RollUpException
	{
		mockTaskStore = mock(RollUpTasksStore.class);
		mockScheduler = mock(KairosDBScheduler.class);
		mockDatastore = mock(KairosDatastore.class);
	}

	@Test
	public void testConstructor_taskStore_null_invalid() throws RollUpException
	{
		expectedException.expect(NullPointerException.class);
		expectedException.expectMessage("taskStore cannot be null");

		new RollUpManager(null, mockScheduler, mockDatastore);
	}

	@Test
	public void testConstructor_dataStore_null_invalid() throws RollUpException
	{
		expectedException.expect(NullPointerException.class);
		expectedException.expectMessage("dataStore cannot be null");

		new RollUpManager(mockTaskStore, mockScheduler, null);
	}

	@Test
	public void testConstructor_loadsExistingTasks() throws KairosDBException
	{
		List<RollupTask> tasks = new ArrayList<RollupTask>();
		tasks.add(createTask("task1"));
		tasks.add(createTask("task2"));
		tasks.add(createTask("task3"));

		when(mockTaskStore.read()).thenReturn(tasks);

		RollUpManager manager = new RollUpManager(mockTaskStore, mockScheduler, mockDatastore);

		verify(mockTaskStore).addListener(manager);
		verify(mockScheduler, times(1)).schedule(RollUpManager.createJobDetail(tasks.get(0), mockDatastore, "localhost"), RollUpManager.createTrigger(tasks.get(0)));
		verify(mockScheduler, times(1)).schedule(RollUpManager.createJobDetail(tasks.get(1), mockDatastore, "localhost"), RollUpManager.createTrigger(tasks.get(1)));
		verify(mockScheduler, times(1)).schedule(RollUpManager.createJobDetail(tasks.get(2), mockDatastore, "localhost"), RollUpManager.createTrigger(tasks.get(2)));
	}

	@Test(expected = NullPointerException.class)
	public void testChange_nullTask_invalid() throws RollUpException
	{
		RollUpManager manager = new RollUpManager(mockTaskStore, mockScheduler, mockDatastore);

		manager.change(null, Action.CHANGED);
	}

	@Test
	public void testChange() throws KairosDBException
	{
		RollupTask added = createTask("task1");
		RollupTask updated = createTask("task2");
		RollupTask removed = createTask("task3");
		RollUpManager manager = new RollUpManager(mockTaskStore, mockScheduler, mockDatastore);

		manager.change(added, Action.ADDED);
		verify(mockScheduler, times(1)).schedule(RollUpManager.createJobDetail(added, mockDatastore, "localhost"), RollUpManager.createTrigger(added));

		manager.change(updated, Action.CHANGED);
		JobDetailImpl job = RollUpManager.createJobDetail(updated, mockDatastore, "localhost");
		verify(mockScheduler, times(1)).cancel(job.getKey());
		verify(mockScheduler, times(1)).schedule(RollUpManager.createJobDetail(updated, mockDatastore, "localhost"), RollUpManager.createTrigger(updated));

		manager.change(removed, Action.REMOVED);
		job = RollUpManager.createJobDetail(removed, mockDatastore, "localhost");
		verify(mockScheduler, times(1)).cancel(job.getKey());
	}

	private static RollupTask createTask(String name)
	{
		Duration duration = new Duration(1, TimeUnit.HOURS);
		List<Rollup> rollups = new ArrayList<Rollup>();
		rollups.add(new Rollup());
		return new RollupTask(name, duration, rollups);
	}
}