package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.KairosRootConfig;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;
import org.kairosdb.core.queue.EventCompletionCallBack;
import org.kairosdb.eventbus.FilterEventBus;
import org.kairosdb.eventbus.Publisher;
import org.kairosdb.events.BatchReductionEvent;
import org.kairosdb.events.DataPointEvent;
import org.kairosdb.events.RowKeyEvent;
import org.mockito.internal.matchers.Any;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BatchHandlerTest
{
	private BatchHandler m_batchHandler;

	private EventCompletionCallBack m_callBack;
	private DataCache<DataPointsRowKey> m_rowKeyDataCache;
	private DataCache<String> m_metricDataCache;

	private Publisher<RowKeyEvent> m_rowKeyEventPublisher;
	private Publisher<BatchReductionEvent> m_batchReductionEventPublisher;
	private CassandraModule.CQLBatchFactory m_cqlBatchFactory;


	private class FakeCQLBatch extends CQLBatch
	{
		private List<DataPointsRowKey> m_newRowKeys = new ArrayList<>();
		private List<String> m_newMetrics = new ArrayList<>();
		private RuntimeException m_exceptionToThrow;

		public FakeCQLBatch(RuntimeException exceptionToThrow)
		{
			super(null, null, null, null);

			m_exceptionToThrow = exceptionToThrow;
		}

		@Override
		public void addRowKey(String metricName, DataPointsRowKey rowKey, int rowKeyTtl)
		{
			m_newRowKeys.add(rowKey);
		}

		@Override
		public void addMetricName(String metricName)
		{
			m_newMetrics.add(metricName);
		}

		@Override
		public void addDataPoint(DataPointsRowKey rowKey, int columnTime, DataPoint dataPoint, int ttl) throws IOException
		{
		}

		@Override
		public void submitBatch()
		{
			if (m_exceptionToThrow != null)
				throw m_exceptionToThrow;
		}

		@Override
		public List<DataPointsRowKey> getNewRowKeys()
		{
			return m_newRowKeys;
		}

		@Override
		public List<String> getNewMetrics()
		{
			return m_newMetrics;
		}
	}

	@SuppressWarnings("unchecked")
	public void setup(List<DataPointEvent> events) throws ParseException
	{
		FilterEventBus eventBus = mock(FilterEventBus.class);
		m_rowKeyEventPublisher = mock(Publisher.class);
		m_batchReductionEventPublisher = mock(Publisher.class);

		m_cqlBatchFactory = mock(CassandraModule.CQLBatchFactory.class);

		when(eventBus.createPublisher(RowKeyEvent.class)).thenReturn(m_rowKeyEventPublisher);
		when(eventBus.createPublisher(BatchReductionEvent.class)).thenReturn(m_batchReductionEventPublisher);
		m_callBack = mock(EventCompletionCallBack.class);

		m_rowKeyDataCache = new DataCache<>(1000);
		m_metricDataCache = new DataCache<>(1000);

		KairosRootConfig rootConfig = new KairosRootConfig();
		//todo setup stuff
		rootConfig.load(ImmutableMap.of("kairosdb.datastore.cassandra.write_cluster", new HashMap()));

		m_batchHandler = new BatchHandler(events,
				m_callBack,
				new CassandraConfiguration(rootConfig),
				m_rowKeyDataCache,
				m_metricDataCache,
				eventBus,
				m_cqlBatchFactory);
	}



	@Test
	public void test_rowKeyPublisher_getsCalled() throws Exception
	{
		LongDataPointFactory dataPointFactory = new LongDataPointFactoryImpl();
		long now = System.currentTimeMillis();

		ImmutableSortedMap<String, String> tags = ImmutableSortedMap.of("host", "bob");
		List<DataPointEvent> events = Arrays.asList(
				new DataPointEvent("metric_name", tags, dataPointFactory.createDataPoint(now, 42L)));

		setup(events);

		when(m_cqlBatchFactory.create()).thenReturn(new FakeCQLBatch(null));

		m_batchHandler.retryCall();

		verify(m_rowKeyEventPublisher).post(any());
	}



	@Test
	public void test_rowKeyCache_gets_cleared() throws Exception
	{
		LongDataPointFactory dataPointFactory = new LongDataPointFactoryImpl();
		long now = System.currentTimeMillis();

		ImmutableSortedMap<String, String> tags = ImmutableSortedMap.of("host", "bob");
		List<DataPointEvent> events = Arrays.asList(
				new DataPointEvent("metric_name", tags, dataPointFactory.createDataPoint(now, 42L)));

		setup(events);

		RuntimeException e = mock(NoHostAvailableException.class);
		when(e.getMessage()).thenReturn("hey");

		when(m_cqlBatchFactory.create()).thenReturn(new FakeCQLBatch(e));

		try
		{
			m_batchHandler.retryCall();
		}
		catch (Exception e1)
		{
		}
		finally
		{
			verify(m_rowKeyEventPublisher).post(any());
			assertThat(m_rowKeyDataCache.getCachedKeys()).isEmpty();
		}
	}


}