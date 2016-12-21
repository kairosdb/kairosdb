package org.kairosdb.core.queue;

import org.kairosdb.core.reporting.KairosMetricReporter;
import org.kairosdb.events.DataPointEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executor;

/**
 Created by bhawkins on 10/12/16.
 */
public abstract class QueueProcessor implements KairosMetricReporter
{
	public static final Logger logger = LoggerFactory.getLogger(QueueProcessor.class);

	public static final String QUEUE_PROCESSOR_CLASS = "kairosdb.queue_processor";
	public static final String QUEUE_PROCESSOR = "queue_processor";
	public static final String BATCH_SIZE = "kairosdb.queue_processor.batch_size";
	public static final String MEMORY_QUEUE_SIZE = "kairosdb.queue_processor.memory_queue_size";


	private final DeliveryThread m_deliveryThread;
	protected final int m_batchSize;

	private volatile ProcessorHandler m_processorHandler;


	public QueueProcessor(Executor executor, int batchSize)
	{
		m_deliveryThread = new DeliveryThread();
		m_batchSize = batchSize;

		executor.execute(m_deliveryThread);
	}


	public void setProcessorHandler(ProcessorHandler processorHandler)
	{
		m_processorHandler = processorHandler;
	}

	public void shutdown()
	{
		m_deliveryThread.shutdown();
	}


	public abstract void put(DataPointEvent dataPointEvent);

	/**
	 @return Returns a Pair containing the latest index
	 and a list of events from the queue, maybe empty
	 */
	protected abstract List<DataPointEvent> get();

	protected abstract EventCompletionCallBack getCompletionCallBack();


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
					List<DataPointEvent> results = get();
					//getCompletionCallBack must be called after get()
					EventCompletionCallBack callbackToPass = getCompletionCallBack();

					//System.out.println(results.size());
					m_processorHandler.handleEvents(results, callbackToPass);
				}
				catch (Exception e)
				{
					logger.error("DeliveryThread Exception", e);
				}
			}
		}
	}







}
