package org.kairosdb.rollup;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.KairosFeatureProcessor;
import org.kairosdb.core.KairosRootConfig;
import org.kairosdb.core.TestDataPointFactory;
import org.kairosdb.core.aggregator.TestAggregatorFactory;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.DatastoreQuery;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.datastore.QueryQueuingManager;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.core.groupby.TestGroupByFactory;
import org.kairosdb.core.http.rest.QueryException;
import org.kairosdb.core.http.rest.json.QueryParser;
import org.kairosdb.core.http.rest.json.TestQueryPluginFactory;
import org.kairosdb.datastore.h2.H2Datastore;
import org.kairosdb.eventbus.EventBusConfiguration;
import org.kairosdb.eventbus.FilterEventBus;
import org.kairosdb.eventbus.Publisher;
import org.kairosdb.events.DataPointEvent;

public class RollupProcessorImplTest
{
	private static final String DB_PATH = "build/h2db_test";
	private static final long MINUTE = 1000 * 60;

	private static KairosDatastore datastore;
	private static FilterEventBus eventBus = new FilterEventBus(new EventBusConfiguration(new KairosRootConfig()));
	private static RollupProcessor processor;
	private static QueryParser queryParser;
	private static Publisher<DataPointEvent> publisher;
	private static RollupTaskStatusStore mockStatusStore;

	@BeforeClass
	public static void setupDatabase() throws KairosDBException, IOException
	{

		KairosDataPointFactory dataPointFactory = new TestDataPointFactory();
		FileUtils.deleteDirectory(new File(DB_PATH));
		H2Datastore h2Datastore = new H2Datastore(DB_PATH, dataPointFactory, eventBus, "regex:");

		datastore = new KairosDatastore(h2Datastore,
				new QueryQueuingManager(1, "hostname"),
				dataPointFactory, false);
		datastore.init();

		eventBus.register(h2Datastore);

		publisher = eventBus.createPublisher(DataPointEvent.class);
		RollUpJob mockJob = mock(RollUpJob.class);
		mockStatusStore = mock(RollupTaskStatusStore.class);
		processor = new RollupProcessorImpl(datastore);
		queryParser = new QueryParser(new KairosFeatureProcessor(
				new TestAggregatorFactory(eventBus), new TestGroupByFactory()),
				new TestQueryPluginFactory());
	}

	@AfterClass
	public static void cleanupDatabase() throws InterruptedException, DatastoreException, IOException
	{
		datastore.close();
		FileUtils.deleteDirectory(new File(DB_PATH));
	}

	/**
	 Looks back 1 hour and 10 minutes(execution interval + sampling)
	 */
	@Test
	public void testNoExistingRollups() throws IOException, QueryException, DatastoreException, InterruptedException, RollUpException
	{
		// Create rollup
		String json = Resources.toString(Resources.getResource("rolluptask1.json"), Charsets.UTF_8);
		RollupTask task = queryParser.parseRollupTask(json);
		Rollup rollup = task.getRollups().get(0);
		QueryMetric query = rollup.getQueryMetrics().get(0);

		// Add data points
		long now = now();

		ImmutableSortedMap<String, String> tags = ImmutableSortedMap.of("host", "foo", "customer", "foobar");
		addDataPoint(query.getName(), tags, now - (69 * MINUTE), 3);
		addDataPoint(query.getName(), tags, now - (65 * MINUTE), 3);
		addDataPoint(query.getName(), tags, now - (59 * MINUTE), 4);
		addDataPoint(query.getName(), tags, now - (55 * MINUTE), 4);
		addDataPoint(query.getName(), tags, now - (49 * MINUTE), 5);
		addDataPoint(query.getName(), tags, now - (45 * MINUTE), 5);
		addDataPoint(query.getName(), tags, now - (39 * MINUTE), 6);
		addDataPoint(query.getName(), tags, now - (35 * MINUTE), 6);
		addDataPoint(query.getName(), tags, now - (29 * MINUTE), 7);
		addDataPoint(query.getName(), tags, now - (25 * MINUTE), 7);
		addDataPoint(query.getName(), tags, now - (19 * MINUTE), 8);
		addDataPoint(query.getName(), tags, now - (15 * MINUTE), 8);

		// Process rollups
		processor.process(mockStatusStore, task, query);

		// Verify 6 rollups created every 10 minutes
		List<DataPoint> rollups = getRollups(rollup.getSaveAs());
		assertThat(rollups.size(), equalTo(6));
		assertThat(rollups.get(0).getLongValue(), equalTo(6L));
		assertThat(rollups.get(1).getLongValue(), equalTo(8L));
		assertThat(rollups.get(2).getLongValue(), equalTo(10L));
		assertThat(rollups.get(3).getLongValue(), equalTo(12L));
		assertThat(rollups.get(4).getLongValue(), equalTo(14L));
		assertThat(rollups.get(5).getLongValue(), equalTo(16L));
	}

	/**
	 Looks back 1 hour and 10 minutes(execution interval + sampling)
	 */
	@Test
	public void testNoExistingRollupsWithSingleDatapoint() throws IOException, QueryException, DatastoreException, InterruptedException, RollUpException
	{
		// Create rollup
		String json = Resources.toString(Resources.getResource("rolluptask2.json"), Charsets.UTF_8);
		RollupTask task = queryParser.parseRollupTask(json);
		Rollup rollup = task.getRollups().get(0);
		QueryMetric query = rollup.getQueryMetrics().get(0);

		// Add data points
		long now = now();
		ImmutableSortedMap<String, String> tags = ImmutableSortedMap.of("host", "foo", "customer", "foobar");
		addDataPoint(query.getName(), tags, now - (49 * MINUTE), 5);

		// Process rollups
		processor.process(mockStatusStore, task, query);

		// Verify 1 rollup
		List<DataPoint> rollups = getRollups(rollup.getSaveAs());
		assertThat(rollups.size(), equalTo(1));
		assertThat(rollups.get(0).getLongValue(), equalTo(5L));
	}

	@Test
	public void testRecentExistingRollup() throws IOException, QueryException, DatastoreException, InterruptedException, RollUpException
	{
		// Create rollup
		String json = Resources.toString(Resources.getResource("rolluptask3.json"), Charsets.UTF_8);
		RollupTask task = queryParser.parseRollupTask(json);
		Rollup rollup = task.getRollups().get(0);
		QueryMetric query = rollup.getQueryMetrics().get(0);

		// Add existing rollup
		long now = now();
		long existingRollupTime = now - (10 * MINUTE);
		addDataPoint(rollup.getSaveAs(),
				ImmutableSortedMap.of("saved_from", query.getName()),
				existingRollupTime, 1);

		// Add data points
		ImmutableSortedMap<String, String> tags = ImmutableSortedMap.of("host", "foo", "customer", "foobar");
		addDataPoint(query.getName(), tags, now - (3 * MINUTE), 1);
		addDataPoint(query.getName(), tags, now - (2 * MINUTE), 2);

		// Process rollups
		processor.process(mockStatusStore, task, query);

		// Verify 1 rollup where both data points are summed together
		List<DataPoint> rollups = getRollups(existingRollupTime + 1000, rollup.getSaveAs());
		assertThat(rollups.size(), equalTo(1));
		assertThat(rollups.get(0).getLongValue(), equalTo(3L));
	}

	@Test
	public void testRecentExistingRollupWithSingleNewDatapoint() throws IOException, QueryException, DatastoreException, InterruptedException, RollUpException
	{
		// Create rollup
		String json = Resources.toString(Resources.getResource("rolluptask4.json"), Charsets.UTF_8);
		RollupTask task = queryParser.parseRollupTask(json);
		Rollup rollup = task.getRollups().get(0);
		QueryMetric query = rollup.getQueryMetrics().get(0);

		// Add existing rollup
		long now = now();
		long existingRollupTime = now - (10 * MINUTE);
		addDataPoint(rollup.getSaveAs(),
				ImmutableSortedMap.of("saved_from", query.getName()),
				existingRollupTime, 1);

		// Add data points
		ImmutableSortedMap<String, String> tags = ImmutableSortedMap.of("host", "foo", "customer", "foobar");
		addDataPoint(query.getName(), tags, now - (2 * MINUTE), 2);

		// Process rollups
		processor.process(mockStatusStore, task, query);

		// Verify 1 rollup where both data points are summed together
		List<DataPoint> rollups = getRollups(existingRollupTime + 1000, rollup.getSaveAs());
		assertThat(rollups.size(), equalTo(1));
		assertThat(rollups.get(0).getLongValue(), equalTo(2L));
	}

	@Test
	public void testExistingRollupButNoDatapointsSince() throws IOException, QueryException, DatastoreException, InterruptedException, RollUpException
	{
		// Create rollup
		String json = Resources.toString(Resources.getResource("rolluptask5.json"), Charsets.UTF_8);
		RollupTask task = queryParser.parseRollupTask(json);
		Rollup rollup = task.getRollups().get(0);
		QueryMetric query = rollup.getQueryMetrics().get(0);

		// Add existing rollup
		long now = now();
		long existingRollupTime = now - (10 * MINUTE);
		addDataPoint(rollup.getSaveAs(),
				ImmutableSortedMap.of("saved_from", query.getName()),
				existingRollupTime, 1);

		// Add NO data points

		// Process rollups
		processor.process(mockStatusStore, task, query);

		// Verify not rollups after existing rollup
		List<DataPoint> rollups = getRollups(existingRollupTime + 1000, rollup.getSaveAs());
		assertThat(rollups.size(), equalTo(0));
	}

	private void addDataPoint(String metricName, ImmutableSortedMap<String, String> tags, long timestamp, int value)
	{
		publisher.post(new DataPointEvent(metricName, tags, new LongDataPoint(timestamp, value)));
	}

	private List<DataPoint> getRollups(String rollupName) throws DatastoreException
	{
		return getRollups(0, rollupName);
	}

	private List<DataPoint> getRollups(long startTime, String rollupName) throws DatastoreException
	{
		List<DataPoint> dataPoints = new ArrayList<>();
		List<DataPointGroup> rollups = queryForRollups(startTime, rollupName);
		for (DataPointGroup dataPointGroup : rollups)
		{
			while(dataPointGroup.hasNext())
			{
				dataPoints.add(dataPointGroup.next());
			}
		}
		return dataPoints;
	}

	private List<DataPointGroup> queryForRollups(long startTime, String rollupName) throws DatastoreException
	{
		try (DatastoreQuery rollupQuery = datastore.createQuery(new QueryMetric(startTime, 0, rollupName)))
		{
			return rollupQuery.execute();
		}
	}

	private long now()
	{
		long now = System.currentTimeMillis();
		return now - now % 600_000L;
	}

}
