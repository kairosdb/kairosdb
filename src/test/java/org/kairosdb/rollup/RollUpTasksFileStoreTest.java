package org.kairosdb.rollup;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.http.rest.json.RelativeTime;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;

public class RollUpTasksFileStoreTest
{
	private static final String DIRECTORY = "build/rolluptaskstore";

	@Before
	public void setup() throws IOException
	{
		FileUtils.deleteDirectory(new File(DIRECTORY));
	}

	@Test(expected = NullPointerException.class)
	public void test_constructor_nullStoreDirectory_invalid()
	{
		new RollUpTasksFileStore(null, parser);
	}

	@Test
	public void test_read_write() throws DatastoreException, RollUpException
	{
		RollUpTasksStore store = new RollUpTasksFileStore(DIRECTORY, parser);

		List<RollUpTask> tasks = new ArrayList<RollUpTask>();
		RollUpTask task1 = new RollUpTask("metric1", "query1", "schedule1", new RelativeTime(1, "days"));
		RollUpTask task2 = new RollUpTask("metric2", "query2", "schedule2", new RelativeTime(2, "weeks"));
		tasks.add(task1);
		tasks.add(task2);

		store.write(tasks);
		Set<RollUpTask> rollUpTasks = store.read();

		assertThat(rollUpTasks.size(), equalTo(2));
		assertRollupTasksContain(rollUpTasks, task2);
		assertRollupTasksContain(rollUpTasks, task1);
	}

	@Test
	public void test_lastModified() throws DatastoreException, InterruptedException, RollUpException
	{
		RollUpTasksStore store = new RollUpTasksFileStore(DIRECTORY, parser);

		List<RollUpTask> tasks = new ArrayList<RollUpTask>();
		RollUpTask task1 = new RollUpTask("metric1", "query1", "schedule1", new RelativeTime(1, "days"));
		tasks.add(task1);

		assertThat(store.lastModifiedTime(), equalTo(0L));

		store.write(tasks);
		long time1 = store.lastModifiedTime();

		Thread.sleep(1000); // anything less than a second fails occasionally
		store.write(tasks);

		assertThat(store.lastModifiedTime(), greaterThan(time1));
	}

	private void assertRollupTasksContain(Set<RollUpTask> tasks, RollUpTask expected)
	{
		for (RollUpTask task : tasks)
		{
			if (task.equals(expected))
			{
				assertThat(task.getMetricName(), equalTo(expected.getMetricName()));
				assertThat(task.getQuery(), equalTo(expected.getQuery()));
				assertThat(task.getSchedule(), equalTo(expected.getSchedule()));
				assertThat(task.getBackfill(), equalTo(expected.getBackfill()));
			}
		}
	}
}