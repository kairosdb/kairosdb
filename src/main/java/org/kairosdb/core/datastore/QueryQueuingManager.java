/*
 * Copyright 2013 Proofpoint Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.kairosdb.core.datastore;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.reporting.KairosMetricReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkArgument;
import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;

public class QueryQueuingManager implements KairosMetricReporter
{
	public static final Logger logger = LoggerFactory.getLogger(QueryQueuingManager.class);
	public static final String CONCURRENT_QUERY_THREAD = "kairosdb.datastore.concurrentQueryThreads";
	public static final String REPORTING_METRIC_NAME = "kairosdb.datastore.queries_waiting";

	private final Map<String, Thread> runningQueries = new HashMap<String, Thread>();
	private final ReentrantLock lock = new ReentrantLock();
	private final Semaphore semaphore;
	private final String hostname;

	private AtomicInteger queriesWaiting = new AtomicInteger();

	@Inject
	public QueryQueuingManager(@Named(CONCURRENT_QUERY_THREAD) int concurrentQueryThreads, @Named("HOSTNAME") String hostname)
	{
		checkArgument(concurrentQueryThreads > 0);
		semaphore = new Semaphore(concurrentQueryThreads, true);
		this.hostname = checkNotNullOrEmpty(hostname);
	}

	public void waitForTimeToRun(String queryHash) throws InterruptedException
	{
		acquireLock(queryHash);
	}

	public void done(String queryHash)
	{
		lock.lock();
		try
		{
			runningQueries.remove(queryHash);
		}
		finally
		{
			lock.unlock();
		}
		semaphore.release();
	}

	private void acquireLock(String queryHash) throws InterruptedException
	{
		if (semaphore.availablePermits() < 1)
		{
			// assume we won't be able to acquire the permit
			queriesWaiting.incrementAndGet();
		}

		semaphore.acquire();

		boolean hashConflict = false;
		lock.lock();
		try
		{
			hashConflict = runningQueries.containsKey(queryHash);
			if (!hashConflict)
			{
				runningQueries.put(queryHash, Thread.currentThread());
			}
		}
		finally
		{
			lock.unlock();
		}

		if (hashConflict)
		{
			semaphore.release();
			queriesWaiting.incrementAndGet();

			while (hashConflict)
			{
				Thread.sleep(100);

				lock.lock();
				try
				{
					hashConflict = runningQueries.containsKey(queryHash);
				}
				finally
				{
					lock.unlock();
				}
			}
			acquireLock(queryHash);
		}
	}

	@Override
	public List<DataPointSet> getMetrics(long now)
	{
		DataPointSet dps = new DataPointSet(REPORTING_METRIC_NAME);
		dps.addTag("host", hostname);
		dps.addTag("method", "put");
		dps.addDataPoint(new DataPoint(now, queriesWaiting.getAndSet(0)));

		return (Collections.singletonList(dps));
	}
}