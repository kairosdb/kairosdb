package org.kairosdb.core.queue;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.eventbus.EventBus;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.TestDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.events.DataPointEvent;
import se.ugli.bigqueue.BigArray;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 Created by bhawkins on 10/15/16.
 */
public class QueueProcessorTest
{
	private LongDataPointFactory m_longDataPointFactory = new LongDataPointFactoryImpl();

	private QueueProcessor.DeliveryThread m_deliveryThread;

	private class TestExecutor implements ExecutorService
	{
		@Override
		public void execute(Runnable command)
		{
			m_deliveryThread = (QueueProcessor.DeliveryThread)command;
		}

		@Override
		public void shutdown()
		{

		}

		@Override
		public List<Runnable> shutdownNow()
		{
			return null;
		}

		@Override
		public boolean isShutdown()
		{
			return false;
		}

		@Override
		public boolean isTerminated()
		{
			return false;
		}

		@Override
		public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
		{
			return false;
		}

		@Override
		public <T> Future<T> submit(Callable<T> task)
		{
			return null;
		}

		@Override
		public <T> Future<T> submit(Runnable task, T result)
		{
			return null;
		}

		@Override
		public Future<?> submit(Runnable task)
		{
			return null;
		}

		@Override
		public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException
		{
			return null;
		}

		@Override
		public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException
		{
			return null;
		}

		@Override
		public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException
		{
			return null;
		}

		@Override
		public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
		{
			return null;
		}
	}

	@Before
	public void setup()
	{
		m_deliveryThread = null;
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void test_bigArray_readingEmptyArray()
	{
		BigArray bigArray = new BigArray("big_array", "kairos_queue", 512*1024*1024);

		long index = bigArray.getTailIndex();
		byte[] data = bigArray.get(index);
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void test_bigArray_readingNonExistingIndex()
	{
		BigArray bigArray = new BigArray("big_array", "kairos_queue", 512*1024*1024);

		long index = bigArray.getTailIndex();
		index ++;
		byte[] data = bigArray.get(index);
	}

	private DataPointEvent createDataPointEvent()
	{
		ImmutableSortedMap<String, String> tags =
				ImmutableSortedMap.<String, String>naturalOrder()
						.put("tag1", "val1")
						.put("tag2", "val2")
						.put("tag3", "val3").build();

		DataPoint dataPoint = m_longDataPointFactory.createDataPoint(123L, 43);
		DataPointEvent event = new DataPointEvent("new_metric", tags, dataPoint, 500);

		return event;
	}


	@Test
	public void test_eventIsPulledFromMemoryQueue() throws DatastoreException
	{
		BigArray bigArray = mock(BigArray.class);

		when(bigArray.append(any())).thenReturn(0L);
		when(bigArray.getTailIndex()).thenReturn(0L);
		when(bigArray.getHeadIndex()).thenReturn(1L);

		DataPointEventSerializer serializer = new DataPointEventSerializer(new TestDataPointFactory());
		ProcessorHandler processorHandler = mock(ProcessorHandler.class);

		QueueProcessor queueProcessor = new FileQueueProcessor(serializer,
				bigArray, new TestExecutor(), 2, 10, 500, 1, 500);

		queueProcessor.setProcessorHandler(processorHandler);

		DataPointEvent event = createDataPointEvent();

		queueProcessor.put(event);

		m_deliveryThread.setRunOnce(true);
		m_deliveryThread.run();

		verify(bigArray, times(1)).append(eq(serializer.serializeEvent(event)));
		verify(processorHandler, times(1)).handleEvents(eq(Arrays.asList(event)), any(), eq(false));
		verify(bigArray, times(0)).get(anyLong());
	}

	@Test
	public void test_eventIsPulledFromMemoryQueueThenBigArray() throws DatastoreException
	{
		BigArray bigArray = mock(BigArray.class);

		when(bigArray.append(any())).thenReturn(0L);
		when(bigArray.getHeadIndex()).thenReturn(2L);

		DataPointEventSerializer serializer = new DataPointEventSerializer(new TestDataPointFactory());
		ProcessorHandler processorHandler = mock(ProcessorHandler.class);

		QueueProcessor queueProcessor = new FileQueueProcessor(serializer,
				bigArray, new TestExecutor(), 3, 1, 500, 1, 500);

		queueProcessor.setProcessorHandler(processorHandler);

		DataPointEvent event = createDataPointEvent();

		queueProcessor.put(event);
		when(bigArray.append(any())).thenReturn(1L);
		queueProcessor.put(event);

		when(bigArray.get(0L)).thenReturn(serializer.serializeEvent(event));
		when(bigArray.get(1L)).thenReturn(serializer.serializeEvent(event));

		m_deliveryThread.setRunOnce(true);
		m_deliveryThread.run();

		verify(bigArray, times(2)).append(eq(serializer.serializeEvent(event)));
		verify(processorHandler, times(1)).handleEvents(eq(Arrays.asList(event, event)), any(), eq(false));
		verify(bigArray, times(1)).get(anyLong());
	}

	@Test
	public void test_checkPointIsCalled() throws DatastoreException
	{
		final EventBus eventBus = mock(EventBus.class);
		BigArray bigArray = mock(BigArray.class);

		when(bigArray.append(any())).thenReturn(0L);
		when(bigArray.getHeadIndex()).thenReturn(2L);

		DataPointEventSerializer serializer = new DataPointEventSerializer(new TestDataPointFactory());
		ProcessorHandler processorHandler = new ProcessorHandler()
		{
			@Override
			public void handleEvents(List<DataPointEvent> events, EventCompletionCallBack eventCompletionCallBack, boolean fullBatch)
			{
				System.out.println("Handling events "+events.size());
				eventCompletionCallBack.complete();
			}
		};

		QueueProcessor queueProcessor = new FileQueueProcessor(serializer,
				bigArray, new TestExecutor(), 3, 2, -1, 1, 500);

		queueProcessor.setProcessorHandler(processorHandler);

		DataPointEvent event = createDataPointEvent();

		queueProcessor.put(event);
		when(bigArray.append(any())).thenReturn(1L);
		queueProcessor.put(event);

		when(bigArray.get(1L)).thenReturn(serializer.serializeEvent(event));

		m_deliveryThread.setRunOnce(true);
		m_deliveryThread.run();

		verify(bigArray, times(2)).append(eq(serializer.serializeEvent(event)));
		//verify(bigArray, times(1)).get(anyLong()); //Item taken from memory
		verify(bigArray, times(1)).removeBeforeIndex(eq(1l));
	}
}
