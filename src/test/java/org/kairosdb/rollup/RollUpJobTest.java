package org.kairosdb.rollup;

import ch.qos.logback.classic.Level;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.KairosFeatureProcessor;
import org.kairosdb.core.aggregator.TestAggregatorFactory;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.core.groupby.TestGroupByFactory;
import org.kairosdb.core.http.rest.QueryException;
import org.kairosdb.core.http.rest.json.QueryParser;
import org.kairosdb.core.http.rest.json.TestQueryPluginFactory;
import org.kairosdb.eventbus.FilterEventBus;
import org.kairosdb.eventbus.Publisher;
import org.kairosdb.events.DataPointEvent;
import org.kairosdb.util.LoggingUtils;
import org.mockito.ArgumentMatcher;
import org.quartz.*;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class RollUpJobTest
{
	private JobExecutionContext mockJobExecutionContext;
	private RollupTaskStatusStore mockStatusStore;
	private KairosDatastore mockDatastore;
	private RollUpJob job;
	private RollupTask task;
	private Level previousLogLevel;

	@SuppressWarnings("unchecked")
	@Before
	public void setup() throws KairosDBException, IOException, QueryException, SchedulerException
	{
		previousLogLevel = LoggingUtils.setLogLevel(Level.OFF);
		Publisher<DataPointEvent> mockPublisher = mock(Publisher.class);
		mockStatusStore = mock(RollupTaskStatusStore.class);
		FilterEventBus mockEventBus = mock(FilterEventBus.class);
		when(mockEventBus.createPublisher(DataPointEvent.class)).thenReturn(mockPublisher);
		mockDatastore = mock(KairosDatastore.class);

		QueryParser queryParser = new QueryParser(new KairosFeatureProcessor(
				new TestAggregatorFactory(mockEventBus), new TestGroupByFactory()),
				new TestQueryPluginFactory());

		String json = Resources.toString(Resources.getResource("rolluptask1.json"), Charsets.UTF_8);
		task = queryParser.parseRollupTask(json);
		ImmutableMap<Object, Object> map = ImmutableMap.of(
				"task", task
		);
		JobDataMap jobDataMap = new JobDataMap(map);

		Scheduler mockScheduler = mock(Scheduler.class);
		when(mockScheduler.getCurrentlyExecutingJobs()).thenReturn(Collections.emptyList());

		mockJobExecutionContext = mock(JobExecutionContext.class);
		when(mockJobExecutionContext.getMergedJobDataMap()).thenReturn(jobDataMap);
		when(mockJobExecutionContext.getScheduler()).thenReturn(mockScheduler);

		job = new RollUpJob(mockDatastore, mockEventBus, "localhost", mockStatusStore);
	}

	@After
	public void tearDown()
	{
		LoggingUtils.setLogLevel(previousLogLevel);
	}

	@Test
	public void testDatastoreException() throws JobExecutionException, DatastoreException, RollUpException
	{
		when(mockDatastore.createQuery(any())).thenThrow(new DatastoreException("ExpectedException"));

		job.execute(mockJobExecutionContext);

		RollupTaskStatus expected = new RollupTaskStatus(new Date(), "localhost");
		RollupQueryMetricStatus foo = new RollupQueryMetricStatus("any",
				"any", 1L, 1L,
				"org.kairosdb.core.exception.DatastoreException: ExpectedException");
		expected.addStatus(foo);
		verify(mockStatusStore).write(eq(task.getId()), argThat(new RollupStatusMatcher(expected)));
	}

	@Test
	public void testRuntimeException() throws JobExecutionException, DatastoreException, RollUpException
	{
		when(mockDatastore.createQuery(any())).thenThrow(new RuntimeException("ExpectedException"));

		job.execute(mockJobExecutionContext);

		RollupTaskStatus expected = new RollupTaskStatus(new Date(), "localhost");
		RollupQueryMetricStatus foo = new RollupQueryMetricStatus("any",
				"any", 1L, 1L,
				"java.lang.RuntimeException: ExpectedException");
		expected.addStatus(foo);
		verify(mockStatusStore).write(eq(task.getId()), argThat(new RollupStatusMatcher(expected)));
	}

	private class RollupStatusMatcher implements ArgumentMatcher<RollupTaskStatus>
	{
		private String errorMessage;
		private RollupTaskStatus expected;

		RollupStatusMatcher(RollupTaskStatus expected)
		{
			this.expected = expected;
		}

		@Override
		public boolean matches(RollupTaskStatus rollupTaskStatus)
		{
			List<RollupQueryMetricStatus> expectedStatuses = expected.getStatuses();
			List<RollupQueryMetricStatus> statuses = rollupTaskStatus.getStatuses();

			if (expectedStatuses.size() != statuses.size())
			{
				errorMessage = "Number of RollupQueryMetricStatuses do not match. Expected "
						+ expectedStatuses.size() + " but was " + statuses.size();
				return false;
			}

			for (RollupQueryMetricStatus expectedStatus : expectedStatuses)
			{
				boolean found = false;
				for (RollupQueryMetricStatus status : statuses)
				{
					if (status.getErrorMessage().startsWith(expectedStatus.getErrorMessage()))
					{
						found = true;
					}
				}
				if (!found)
				{
					errorMessage = "Error messages do no match. Expected " + expectedStatus.getErrorMessage();
					return false;
				}
			}

			return true;
		}

		@Override
		public String toString()
		{
			if (errorMessage != null)
			{
				return errorMessage;
			}
			return "";
		}
	}
}

