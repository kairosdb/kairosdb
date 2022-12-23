package org.kairosdb.core.queue;

import org.kairosdb.core.KairosPostConstructInit;
import org.kairosdb.events.DataPointEvent;
import org.kairosdb.metrics4j.MetricSourceManager;
import org.kairosdb.metrics4j.annotation.Reported;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 Created by bhawkins on 12/15/16.
 */
public class MemoryQueueProcessor extends QueueProcessor implements KairosPostConstructInit// implements KairosMetricReporter
{
	public static final Logger logger = LoggerFactory.getLogger(MemoryQueueProcessor.class);
	public static final QueueStats stats = MetricSourceManager.getSource(QueueStats.class);

	private static final EventCompletionCallBack CALL_BACK = new VoidCompletionCallBack();

	private final BlockingQueue<DataPointEvent> m_queue;


	@Inject
	public MemoryQueueProcessor(
			@Named(QUEUE_PROCESSOR) ExecutorService executor,
			@Named(BATCH_SIZE) int batchSize,
			@Named(MEMORY_QUEUE_SIZE) int memoryQueueSize,
			@Named(MINIMUM_BATCH_SIZE) int minimumBatchSize,
			@Named(MINIMUM_BATCH_WAIT) int minBatchWait)

	{
		super(executor, batchSize, minimumBatchSize, minBatchWait);

		m_queue = new ArrayBlockingQueue<>(memoryQueueSize, true);

		MetricSourceManager.addSource(QueueStats.class.getName(),
				"memoryQueueSize", Collections.emptyMap(), "Amount of data in the memory queue", () -> m_queue.size());
	}

	@Override
	public void init()
	{
	}


	@Override
	public void put(DataPointEvent dataPointEvent)
	{
		try
		{
			m_queue.put(dataPointEvent);
		}
		catch (InterruptedException e)
		{
			logger.error("Error putting data", e);
		}
	}

	@Override
	protected int getAvailableDataPointEvents()
	{
		return m_queue.size();
	}

	@Override
	protected List<DataPointEvent> get(int batchSize)
	{
		List<DataPointEvent> ret = new ArrayList<>(batchSize/4);
		try
		{
			ret.add(m_queue.take());
			//Thread.sleep(50);
		}
		catch (InterruptedException e)
		{
			logger.error("Error taking from queue", e);
		}
		m_queue.drainTo(ret, batchSize -1);

		stats.processCount("memory").put(ret.size());
		return ret;
	}

	@Override
	protected EventCompletionCallBack getCompletionCallBack()
	{
		return CALL_BACK;
	}


	private static class VoidCompletionCallBack implements EventCompletionCallBack
	{
		@Override
		public void complete()
		{
			//does nothing
		}
	}
}
