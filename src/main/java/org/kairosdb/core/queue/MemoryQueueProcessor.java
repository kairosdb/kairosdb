package org.kairosdb.core.queue;

import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;
import org.kairosdb.core.reporting.KairosMetricReporter;
import org.kairosdb.events.DataPointEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 Created by bhawkins on 12/15/16.
 */
public class MemoryQueueProcessor extends QueueProcessor implements KairosMetricReporter
{
	public static final Logger logger = LoggerFactory.getLogger(MemoryQueueProcessor.class);
	private static final EventCompletionCallBack CALL_BACK = new VoidCompletionCallBack();

	private AtomicInteger m_readFromQueueCount = new AtomicInteger();
	private final BlockingQueue<DataPointEvent> m_queue;

	@Inject @Named("HOSTNAME")
	private String m_hostName = "none";

	@Inject
	private LongDataPointFactory m_dataPointFactory = new LongDataPointFactoryImpl();

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
	}

	@Override
	public void addReportedMetrics(ArrayList<DataPointSet> metrics, long now)
	{
		long readFromQueue = m_readFromQueueCount.getAndSet(0);
		long arraySize = m_queue.size();

		DataPointSet dps = new DataPointSet("kairosdb.queue.process_count");
		dps.addTag("host", m_hostName);
		dps.addDataPoint(m_dataPointFactory.createDataPoint(now, readFromQueue));

		metrics.add(dps);

		dps = new DataPointSet("kairosdb.queue.memory_queue.size");
		dps.addTag("host", m_hostName);
		dps.addDataPoint(m_dataPointFactory.createDataPoint(now, arraySize));

		metrics.add(dps);
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

		//System.out.println(ret.size());
		m_readFromQueueCount.getAndAdd(ret.size());
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
