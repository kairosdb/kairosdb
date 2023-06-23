package org.kairosdb.util;

import com.datastax.oss.driver.api.core.servererrors.UnavailableException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.base.Stopwatch;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.datapoints.DoubleDataPointFactoryImpl;
import org.kairosdb.eventbus.Subscribe;
import org.kairosdb.events.ShutdownEvent;
import org.kairosdb.metrics4j.MetricSourceManager;
import org.kairosdb.metrics4j.collectors.DurationCollector;

import javax.inject.Inject;
import javax.inject.Named;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 Created by bhawkins on 10/27/16.
 */
public class IngestExecutorService
{
	public interface IngestStats
	{
		DurationCollector writeTimeMicro();
	}

	private static final IngestStats stats = MetricSourceManager.getSource(IngestStats.class);
	public static final String PERMIT_COUNT = "kairosdb.ingest_executor.thread_count";

	private final ExecutorService m_internalExecutor;
	private final ThreadGroup m_threadGroup;
	//Original idea behind this is that the number of threads could
	//adjust via incrementing or decrementing the semaphore count.
	private final CongestionSemaphore m_semaphore;
	private int m_permitCount = 10;
	private final Retryer<Integer> m_retryer;

	@Inject
	private DoubleDataPointFactory m_dataPointFactory = new DoubleDataPointFactoryImpl();


	@Inject
	public IngestExecutorService(@Named(PERMIT_COUNT) int permitCount)
	{
		m_permitCount = permitCount;
		//m_congestionTimer = new CongestionTimer(m_permitCount);
		m_semaphore = new CongestionSemaphore(m_permitCount);
		m_threadGroup = new ThreadGroup("KairosDynamic");
		m_internalExecutor = Executors.newCachedThreadPool(new ThreadFactory()
		{
			private int m_count = 0;
			@Override
			public Thread newThread(Runnable r)
			{
				Thread t = new Thread(m_threadGroup, r,"Ingest worker-"+m_count++);
				//t.setDaemon(true);
				return t;
			}
		});

		m_retryer = RetryerBuilder.<Integer>newBuilder()
				//.retryIfExceptionOfType(NoHostAvailableException.class)
				.retryIfExceptionOfType(UnavailableException.class)
				.withWaitStrategy(WaitStrategies.fibonacciWait(1, TimeUnit.MINUTES))
				.build();
	}

	/*private void increasePermitCount()
	{
		m_permitCount ++;
		//m_congestionTimer.setTaskPerBatch(m_permitCount);
		m_semaphore.release();
	}*/

	@Subscribe
	public void shutdown(ShutdownEvent event)
	{
		shutdown();
	}

	public void shutdown()
	{
		m_internalExecutor.shutdown();
	}


	private Stopwatch m_timer = Stopwatch.createStarted();

	/**
	 Calls to submit will block until a permit is available to process the request
	 @param callable
	 */
	public void submit(Callable<Integer> callable)
	{
		try
		{
			//System.out.println("Execute called");
			m_semaphore.acquire();
			//System.out.println("Submitting");
			m_internalExecutor.submit(
					new IngestFutureTask(m_retryer.wrap(callable)));
			//System.out.println("Done submitting");
		}
		//Potentially thrown by acquire
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
	}


	private class IngestFutureTask extends FutureTask<Integer>
	{
		private final Stopwatch m_stopwatch;

		public IngestFutureTask(Callable<Integer> callable)
		{
			super(callable);
			m_stopwatch = Stopwatch.createUnstarted();
		}

		@Override
		public void run()
		{
			try
			{
				m_stopwatch.start();
				super.run();
				m_stopwatch.stop();

				stats.writeTimeMicro().put(Duration.ofNanos(m_stopwatch.elapsed(TimeUnit.NANOSECONDS)));
			}
			finally
			{
				m_semaphore.release();
			}
		}

		@Override
		public void set(Integer retries)
		{
			//Todo Calculate time to run and adjust number of threads
			/*if (full)
			{
			}*/

			super.set(retries);
		}
	}

	private static class CongestionSemaphore extends Semaphore
	{
		public CongestionSemaphore(int permits)
		{
			super(permits);
		}

		public void reducePermits(int reduction)
		{
			super.reducePermits(reduction);
		}
	}
}
