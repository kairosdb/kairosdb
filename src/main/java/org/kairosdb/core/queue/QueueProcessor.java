package org.kairosdb.core.queue;

import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.eventbus.Subscribe;
import org.kairosdb.events.BatchReductionEvent;
import org.kairosdb.events.DataPointEvent;
import org.kairosdb.metrics4j.MetricSourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 Created by bhawkins on 10/12/16.
 */
public abstract class QueueProcessor
{
	public static final Logger logger = LoggerFactory.getLogger(QueueProcessor.class);
	public static final QueueStats stats = MetricSourceManager.getSource(QueueStats.class);

	public static final String QUEUE_PROCESSOR_CLASS = "kairosdb.queue_processor.class";
	public static final String QUEUE_PROCESSOR = "queue_processor";
	public static final String BATCH_SIZE = "kairosdb.queue_processor.batch_size";
	public static final String MEMORY_QUEUE_SIZE = "kairosdb.queue_processor.memory_queue_size";
	public static final String MINIMUM_BATCH_SIZE = "kairosdb.queue_processor.min_batch_size";
	public static final String MINIMUM_BATCH_WAIT = "kairosdb.queue_processor.min_batch_wait";


	private final DeliveryThread m_deliveryThread;
	private final ExecutorService m_executor;
	private int m_batchSize;
	private final int m_initialBatchSize;
	private final int m_minimumBatchSize;
	private final int m_minBatchWait;

	private volatile ProcessorHandler m_processorHandler;


	public QueueProcessor(ExecutorService executor, int batchSize, int minimumBatchSize,
			int minBatchWait)
	{
		m_deliveryThread = new DeliveryThread();
		m_initialBatchSize = m_batchSize = batchSize;
		m_minimumBatchSize = minimumBatchSize;
		m_minBatchWait = minBatchWait;

		executor.execute(m_deliveryThread);
		m_executor = executor;
		logger.info("Starting QueueProcessor "+this.getClass().getName());
	}

	@Subscribe
	public void reduceBatch(BatchReductionEvent reductionEvent)
	{
		m_batchSize = Math.min(m_batchSize, reductionEvent.getBatchSize());

		logger.info("Reducing queue batch size to "+m_batchSize);
	}


	public void setProcessorHandler(ProcessorHandler processorHandler)
	{
		m_processorHandler = processorHandler;
	}

	public void shutdown()
	{
		m_deliveryThread.shutdown();
		m_executor.shutdown();
	}


	public abstract void put(DataPointEvent dataPointEvent) throws DatastoreException;

	/**
	 @return Returns a Pair containing the latest index
	 and a list of events from the queue, maybe empty
	 */
	protected abstract List<DataPointEvent> get(int batchSize);

	protected abstract int getAvailableDataPointEvents();

	protected abstract EventCompletionCallBack getCompletionCallBack();


	/**
	 Single thread that pulls data out of the queue and sends it to the callback
	 in batches
	 */
	public class DeliveryThread implements Runnable
	{
		private boolean m_running = true;
		private boolean m_runOnce = false;

		public DeliveryThread()
		{
		}

		public void shutdown()
		{
			m_running = false;
		}

		/**
		 Used for testing the queue processor to clear the running state
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
			try
			{
				//to fix race condition on startup
				while (m_processorHandler == null)
					Thread.sleep(100);
			}
			catch (InterruptedException e)
			{
				e.printStackTrace();
			}

			while (m_running)
			{
				if (m_runOnce)
					m_running = false;

				try
				{
					if (getAvailableDataPointEvents() < m_minimumBatchSize)
					{
						Thread.sleep(m_minBatchWait);
					}

					if (getAvailableDataPointEvents() == 0)
						continue;

					List<DataPointEvent> results = get(m_batchSize);
					//getCompletionCallBack must be called after get()
					EventCompletionCallBack callbackToPass = getCompletionCallBack();

					stats.batchStats().put(results.size());

					boolean fullBatch = false;

					if (results.size() == m_batchSize)
					{
						fullBatch = true;
						if (m_batchSize < m_initialBatchSize)
							m_batchSize += 5;
					}

					m_processorHandler.handleEvents(results, callbackToPass, fullBatch);
				}
				catch (Exception e)
				{
					logger.error("DeliveryThread Exception", e);
				}
			}
		}
	}







}
