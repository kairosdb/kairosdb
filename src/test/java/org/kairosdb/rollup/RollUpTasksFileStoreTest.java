package org.kairosdb.rollup;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.kairosdb.core.aggregator.TestAggregatorFactory;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.core.groupby.TestGroupByFactory;
import org.kairosdb.core.http.rest.json.QueryParser;
import org.kairosdb.core.http.rest.json.TestQueryPluginFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RollUpTasksFileStoreTest
{
	private static final String DIRECTORY = "build/rolluptaskstore";

	private QueryParser parser;
	private List<RollupTaskTarget> targets = new ArrayList<RollupTaskTarget>();

	@Before
	public void setup() throws IOException, KairosDBException
	{
		FileUtils.deleteDirectory(new File(DIRECTORY));
		parser = new QueryParser(new TestAggregatorFactory(), new TestGroupByFactory(), new TestQueryPluginFactory());

		RollupTaskTarget target1 = new RollupTaskTarget("target");
//		target1.addAggregator(new SumAggregator()); // todo fix this
		targets.add(target1);
	}

	//	@Test(expected = NullPointerException.class)
	//	public void test_constructor_nullStoreDirectory_invalid() throws IOException
	//	{
	//		new RollUpTasksFileStore(null, parser);
	//	}

	//	@Test
	//	public void test_read_write() throws DatastoreException, RollUpException, IOException
	//	{
	//		RollUpTasksStore store = new RollUpTasksFileStore(DIRECTORY, parser);
	//
	//		List<RollUpTask> tasks = new ArrayList<RollUpTask>();
	//		RollUpTask task1 = new RollUpTask("metric1", new RelativeTime(1, "days"),
	//				targets, "schedule1");
	//		RollUpTask task2 = new RollUpTask("metric2", new RelativeTime(2, "weeks"),
	//				targets, "schedule2");
	//		task2.addFilter("host", "host1");
	//		task2.addFilter("host", "host2");
	//		task2.addFilter("customer", "customer1");
	//
	//		// todo add groupbys
	//		tasks.add(task1);
	//		tasks.add(task2);
	//
	//		store.write(tasks);
	//		List<RollUpTask> rollUpTasks = store.read();
	//
	//		assertThat(rollUpTasks.size(), equalTo(2));
	//		assertRollupTasksContain(rollUpTasks, task2);
	//		assertRollupTasksContain(rollUpTasks, task1);
	//	}
	//
	//	@Test
	//	public void test_lastModified() throws DatastoreException, InterruptedException, RollUpException, IOException
	//	{
	//		RollUpTasksStore store = new RollUpTasksFileStore(DIRECTORY, parser);
	//		List<RollupTaskTarget> targets = new ArrayList<RollupTaskTarget>();
	//		targets.add(new RollupTaskTarget("target1"));
	//
	//		List<RollUpTask> tasks = new ArrayList<RollUpTask>();
	//		RollUpTask task1 = new RollUpTask("metric1", new RelativeTime(1, "days"),
	//				targets, "schedule1");
	//		tasks.add(task1);
	//
	//		assertThat(store.lastModifiedTime(), equalTo(0L));
	//
	//		store.write(tasks);
	//		long time1 = store.lastModifiedTime();
	//
	//		Thread.sleep(1000); // anything less than a second fails occasionally
	//		store.write(tasks);
	//
	//		assertThat(store.lastModifiedTime(), greaterThan(time1));
	//	}
	//
	//	private void assertRollupTasksContain(List<RollUpTask> tasks, RollUpTask expected)
	//	{
	//		for (RollUpTask task : tasks)
	//		{
	//			if (task.equals(expected))
	//			{
	//				assertThat(task.getMetricName(), equalTo(expected.getMetricName()));
	//				assertThat(task.getStartTime(), equalTo(expected.getStartTime()));
	//				assertThat(task.getSchedule(), equalTo(expected.getSchedule()));
	//
	//				assertThat(task.getFilters().size(), equalTo(expected.getFilters().size()));
	//				for (String name : expected.getFilters().keys())
	//				{
	//					assertTrue(task.getFilters().containsKey(name));
	//					assertThat(task.getFilters().get(name), equalTo(expected.getFilters().get(name)));
	//				}
	//
	//				assertThat(task.getTargets().size(), equalTo(expected.getTargets().size()));
	//				for (RollupTaskTarget expectedTarget : expected.getTargets())
	//				{
	//					for (RollupTaskTarget actualTarget : task.getTargets())
	//					{
	//						assertTarget(expectedTarget, actualTarget);
	//					}
	//				}
	//			}
	//		}
	//	}
	//
	//	private void assertTarget(RollupTaskTarget expected, RollupTaskTarget actual)
	//	{
	//		assertThat(actual.getName(), equalTo(expected.getName()));
	//		assertThat(actual.getTags(), equalTo(expected.getTags()));
	//		assertThat(actual.getAggregators(), equalTo(expected.getAggregators()));
	//	}
}