package org.kairosdb.rollup;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.KairosQueryProcessingChain;
import org.kairosdb.core.aggregator.TestAggregatorFactory;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.core.groupby.TestGroupByFactory;
import org.kairosdb.core.http.rest.QueryException;
import org.kairosdb.core.http.rest.json.QueryParser;
import org.kairosdb.core.http.rest.json.TestQueryPluginFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.kairosdb.rollup.RollupTaskChangeListener.Action;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class RollUpTasksFileStoreTest
{
	private static final String DIRECTORY = "build/rolluptaskstore";

	private QueryParser parser;
	private RollupTaskChangeListener mockListener;

	@Before
	public void setup() throws IOException, KairosDBException
	{
		FileUtils.deleteDirectory(new File(DIRECTORY));
		parser = new QueryParser(new KairosQueryProcessingChain(new TestAggregatorFactory(), new TestGroupByFactory()), new TestQueryPluginFactory());

		mockListener = mock(RollupTaskChangeListener.class);
	}

	@Test(expected = NullPointerException.class)
	public void test_constructor_nullStoreDirectory_invalid() throws IOException, RollUpException
	{
		new RollUpTasksFileStore(null, parser);
	}

	@Test(expected = NullPointerException.class)
	public void test_constructor_nullStoreParser_invalid() throws IOException, RollUpException
	{
		new RollUpTasksFileStore(DIRECTORY, null);
	}

	@Test(expected = NullPointerException.class)
	public void test_write_nullTaskList_invalid() throws IOException, RollUpException
	{
		new RollUpTasksFileStore(DIRECTORY, null).write(null);
	}

	@Test
	public void test_read_write() throws IOException, RollUpException, QueryException
	{
		List<RollupTask> tasks = createTasks();
		Collections.sort(tasks, new TaskComparator());

		RollUpTasksStore store = new RollUpTasksFileStore(DIRECTORY, parser);
		store.addListener(mockListener);

		store.write(tasks);
		List<RollupTask> readTasks = store.read();
		Collections.sort(readTasks, new TaskComparator());

		assertThat(readTasks.size(), equalTo(tasks.size()));
		assertEquals(readTasks, tasks);

		for (RollupTask task : tasks)
		{
			verify(mockListener).change(task, Action.ADDED);
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void test_remove_emptyId_invalid() throws IOException, RollUpException
	{
		new RollUpTasksFileStore(DIRECTORY, parser).remove("");
	}

	@Test(expected = NullPointerException.class)
	public void test_remove_nullId_invalid() throws IOException, RollUpException
	{
		new RollUpTasksFileStore(DIRECTORY, null).remove(null);
	}

	@Test
	public void test_remove() throws IOException, QueryException, RollUpException
	{
		List<RollupTask> tasks = createTasks();

		RollUpTasksStore store = new RollUpTasksFileStore(DIRECTORY, parser);
		store.addListener(mockListener);
		store.write(tasks);

		assertThat(store.read().size(), equalTo(2));

		store.remove(tasks.get(1).getId());
		assertThat(store.read().size(), equalTo(1));
		assertThat(store.read(), hasItem(tasks.get(0)));
		verify(mockListener).change(tasks.get(1), Action.REMOVED);

		store.remove(tasks.get(0).getId());
		assertThat(store.read().size(), equalTo(0));
		verify(mockListener).change(tasks.get(0), Action.REMOVED);
	}

	private List<RollupTask> createTasks() throws IOException, QueryException
	{
		List<RollupTask> tasks = new ArrayList<RollupTask>();
		String json = Resources.toString(Resources.getResource("rolluptask1.json"), Charsets.UTF_8);
		tasks.add(parser.parseRollupTask(json));
		json = Resources.toString(Resources.getResource("rolluptask2.json"), Charsets.UTF_8);
		tasks.add(parser.parseRollupTask(json));
		return tasks;
	}

	private class TaskComparator implements java.util.Comparator<RollupTask>
	{
		@Override
		public int compare(RollupTask task1, RollupTask task2)
		{
			return task1.getId().compareTo(task2.getId());
		}
	}
}