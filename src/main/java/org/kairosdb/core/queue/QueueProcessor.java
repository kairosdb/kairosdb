package org.kairosdb.core.queue;

import com.google.common.base.Stopwatch;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.commons.lang3.tuple.Pair;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;
import org.kairosdb.core.reporting.KairosMetricReporter;
import org.kairosdb.events.DataPointEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.ugli.bigqueue.BigArray;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 Created by bhawkins on 10/12/16.
 */
public class QueueProcessor implements KairosMetricReporter
{
	public static final Logger logger = LoggerFactory.getLogger(QueueProcessor.class);

	public static final String QUEUE_PROCESSOR = "queue_processor";
	public static final String BATCH_SIZE = "kairosdb.queue_processor.batch_size";
	public static final String MEMORY_QUEUE_SIZE = "kairosdb.queue_processor.memory_queue_size";
	public static final String SECONDS_TILL_CHECKPOINT = "kairosdb.queue_processor.seconds_till_checkpoint";


	private final Object m_lock = new Object();
	private final BigArray m_bigArray;
	private final CircularFifoQueue<IndexedEvent> m_memoryQueue;
	private final DeliveryThread m_deliveryThread;
	private final int m_batchSize;
	private final int m_secondsTillCheckpoint;
	private final DataPointEventSerializer m_eventSerializer;

	private AtomicInteger m_readFromFileCount = new AtomicInteger();

	private ProcessorHandler m_processorHandler;
	private long m_nextIndex = -1L;

	@Inject @Named("HOSTNAME")
	private String m_hostName = "none";

	@Inject
	private LongDataPointFactory m_dataPointFactory = new LongDataPointFactoryImpl();

	@Inject
	public QueueProcessor(
			DataPointEventSerializer eventSerializer,
			BigArray bigArray,
			@Named(QUEUE_PROCESSOR) Executor executor,
			@Named(BATCH_SIZE) int batchSize,
			@Named(MEMORY_QUEUE_SIZE) int memoryQueueSize,
			@Named(SECONDS_TILL_CHECKPOINT) int secondsTillCheckpoint)
	{
		m_bigArray = bigArray;
		m_memoryQueue = new CircularFifoQueue<>(memoryQueueSize);
		m_deliveryThread = new DeliveryThread();
		m_batchSize = batchSize;
		m_secondsTillCheckpoint = secondsTillCheckpoint;
		m_eventSerializer = eventSerializer;
		m_nextIndex = m_bigArray.getTailIndex();
		System.out.println("Next index: "+m_nextIndex);

		executor.execute(m_deliveryThread);
	}


	public void setProcessorHandler(ProcessorHandler processorHandler)
	{
		m_processorHandler = processorHandler;
	}

	public void shutdown()
	{
		//todo: would like to drain the queue before shutting down.
		m_deliveryThread.shutdown();
		m_bigArray.flush();
		m_bigArray.close();
		//m_deliveryThread.interrupt();
	}

	public void put(DataPointEvent dataPointEvent)
	{
		byte[] eventBytes = m_eventSerializer.serializeEvent(dataPointEvent);

		synchronized (m_lock)
		{
			//Add data to bigArray first
			long index = m_bigArray.append(eventBytes);

			//Then stick it into the in memory queue
			m_memoryQueue.add(new IndexedEvent(dataPointEvent, index));

			//Notify the reader thread if it is waiting for data
			m_lock.notify();
		}
	}

	/**

	 @return Returns a Pair containing the latest index
	 and a list of events from the queue, maybe empty
	 */
	private Pair<Long, List<DataPointEvent>> get()
	{
		List<DataPointEvent> ret = new ArrayList<>();
		long returnIndex = 0L;

		synchronized (m_lock)
		{
			for (int i = 0; i < m_batchSize; i++)
			{
				//System.out.println(m_nextIndex);
				IndexedEvent event = m_memoryQueue.peek();
				if (event != null && event.m_index == m_nextIndex)
				{
					m_memoryQueue.remove();
				}
				else
				{
					if (m_nextIndex != m_bigArray.getHeadIndex())
					{
						DataPointEvent dataPointEvent = m_eventSerializer.deserializeEvent(m_bigArray.get(m_nextIndex));
						event = new IndexedEvent(dataPointEvent, m_nextIndex);
						m_readFromFileCount.incrementAndGet();
					}
				}

				if (event != null)
				{
					returnIndex = m_nextIndex;
					m_nextIndex = incrementIndex(m_nextIndex);
					ret.add(event.m_dataPointEvent);
				}
				else
					break; //exhausted queue

			}
		}

		return Pair.of(returnIndex, ret);
	}

	private long incrementIndex(long index)
	{
		if (index == Long.MAX_VALUE)
			return 0;

		return index + 1;
	}

	private void waitForEvent()
	{
		synchronized (m_lock)
		{
			if (m_memoryQueue.isEmpty())
			{
				try
				{
					m_lock.wait();
				}
				catch (InterruptedException e)
				{
					logger.info("Queue processor sleep interrupted");
				}
			}
		}

		try
		{
			//Adding sleep after waiting for data helps ensure we batch incoming
			//data instead of getting the first one right off and sending it alone
			Thread.sleep(100);
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}

	}

	@Override
	public List<DataPointSet> getMetrics(long now)
	{
		List<DataPointSet> ret = new ArrayList<>();

		DataPointSet dps = new DataPointSet("kairosdb.queue.file_queue.size");
		dps.addTag("host", m_hostName);
		dps.addDataPoint(m_dataPointFactory.createDataPoint(now, m_bigArray.size()));

		ret.add(dps);

		dps = new DataPointSet("kairosdb.queue.read_from_file");
		dps.addTag("host", m_hostName);
		dps.addDataPoint(m_dataPointFactory.createDataPoint(now, m_readFromFileCount.getAndSet(0)));

		ret.add(dps);

		return ret;
	}


	/**
	 Single thread that pulls data out of the queue and sends it to the callback
	 in batches
	 */
	public class DeliveryThread implements Runnable
	{
		private boolean m_running = true;
		private boolean m_runOnce = false;

		public void shutdown()
		{
			m_running = false;
		}

		/**
		 Used for testing the queue processor to reset the running state
		 @param running
		 */
		public void setRunning(boolean running)
		{
			m_running = running;
		}

		/**
		 Used for testing the queue processor
		 @param runOnce
		 */
		public void setRunOnce(boolean runOnce)
		{
			m_runOnce = runOnce;
		}

		@Override
		public void run()
		{
			CompletionCallBack completionCallBack = new CompletionCallBack();
			Stopwatch stopwatch = Stopwatch.createStarted();

			while (m_running)
			{
				if (m_runOnce)
					m_running = false;

				try
				{
					//Fix race condition on startup
					if (m_processorHandler == null)
						waitForEvent();

					CompletionCallBack callbackToPass = completionCallBack;
					Pair<Long, List<DataPointEvent>> results = get();

					//check if list is not empty
					if (results.getRight().isEmpty())
					{
						waitForEvent();
					}
					else
					{
						//Important to call increment before potentially calling finalize
						callbackToPass.increment();

						//System.out.println("timer: "+stopwatch.elapsed(TimeUnit.SECONDS));
						if (stopwatch.elapsed(TimeUnit.SECONDS) > m_secondsTillCheckpoint)
						{
							System.out.println("Checkpoint");
							callbackToPass.finalize(results.getLeft());
							completionCallBack = new CompletionCallBack();
							callbackToPass.setChildCallBack(completionCallBack);
							stopwatch.reset();
							stopwatch.start();
						}

						m_processorHandler.handleEvents(results.getRight(), callbackToPass);
					}
				}
				catch (Exception e)
				{
					logger.error("DeliveryThread Exception", e);
				}
			}
		}
	}

	/**
	 The purpose of this class is to track all batches sent up to a certain point
	 and once they are finished (via a call to complete) this will move the
	 tail of the big array.
	 */
	private class CompletionCallBack implements EventCompletionCallBack
	{
		private long m_completionIndex;
		private final AtomicInteger m_counter;
		private volatile boolean m_finalized;
		private CompletionCallBack m_childCallBack;

		private CompletionCallBack()
		{
			m_counter = new AtomicInteger(0);
			m_finalized = false;
		}

		public void setChildCallBack(CompletionCallBack childCallBack)
		{
			m_childCallBack = childCallBack;
			m_childCallBack.increment();
		}

		public void increment()
		{
			m_counter.incrementAndGet();
		}

		/**
		 The finalize method gets called always before the last call to complete
		 No need for locking
		 @param completionIndex
		 */
		public void finalize(long completionIndex)
		{
			m_finalized = true;
			m_completionIndex = completionIndex;
		}

		@Override
		public void complete()
		{
			if (m_counter.decrementAndGet() == 0 && m_finalized)
			{
				System.out.println("Setting index");
				m_childCallBack.complete();
				//Checkpoint big queue
				m_bigArray.removeBeforeIndex(m_completionIndex);
			}
		}
	}


	/**
	 Holds a DataPointEvent and the index it is at in the BigArray.
	 Basically to keep the in memory circular queue and BigArray in sync.
	 */
	private static class IndexedEvent
	{
		public final DataPointEvent m_dataPointEvent;
		public final long m_index;

		public IndexedEvent(DataPointEvent dataPointEvent, long index)
		{
			m_dataPointEvent = dataPointEvent;
			m_index = index;
		}
	}


}
