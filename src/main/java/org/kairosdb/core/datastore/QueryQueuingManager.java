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
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.reporting.KairosMetricReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkArgument;
import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;

public class QueryQueuingManager implements KairosMetricReporter
{
	public static final Logger logger = LoggerFactory.getLogger(QueryQueuingManager.class);
	public static final String CONCURRENT_QUERY_THREAD = "kairosdb.datastore.concurrentQueryThreads";
	public static final String QUERY_COLLISIONS_METRIC_NAME = "kairosdb.datastore.query_collisions";
	public static final String QUERY_AVAILABLE_PERMITS = "kairosdb.datastore.available_permits";
	public static final String QUERY_QUEUE_LENGTH = "kairosdb.datastore.query_queue_length";
	public static final String ARTIFACT_VERSION = "kairosdb.datastore.artifact.version";
	public static final String DEPLOYMENT_ID = "kairosdb.datastore.deployment.id";

	@Inject(optional = true)
	@Named(ARTIFACT_VERSION)
	private String m_artifactVersion = "2.0-z";

	@Inject(optional = true)
	@Named(DEPLOYMENT_ID)
	private String m_deploymentId = "2.0-z-d1";

	private final Map<String, Thread> runningQueries = new HashMap<String, Thread>();
	private final ReentrantLock lock = new ReentrantLock();
	private final Semaphore semaphore;
	private final String hostname;

	private AtomicInteger collisions = new AtomicInteger();

	@Inject
	public QueryQueuingManager(@Named(CONCURRENT_QUERY_THREAD) int concurrentQueryThreads, @Named("HOSTNAME") String hostname)
	{
		checkArgument(concurrentQueryThreads > 0);
		this.hostname = checkNotNullOrEmpty(hostname);
		semaphore = new Semaphore(concurrentQueryThreads, true);
	}

	public void waitForTimeToRun(String queryHash) throws InterruptedException
	{
		boolean firstTime = true;
		while(!acquireSemaphore(queryHash))
		{
			if (firstTime)
			{
				collisions.incrementAndGet();
				firstTime = false;
			}
			Thread.sleep(100);
		}
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

	private boolean acquireSemaphore(String queryHash) throws InterruptedException
	{
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
			return false;
		}
		else
			return true;
	}

	public int getQueryWaitingCount()
	{
		return semaphore.getQueueLength();
	}

	public int getAvailableThreads()
	{
		return semaphore.availablePermits();
	}

	@Override
	public List<DataPointSet> getMetrics(long now)
	{
		DataPointSet collisionSet = new DataPointSet(QUERY_COLLISIONS_METRIC_NAME);
		DataPointSet permitSet = new DataPointSet(QUERY_AVAILABLE_PERMITS);
		DataPointSet queueLengthSet = new DataPointSet(QUERY_QUEUE_LENGTH);

		collisionSet.addTag("host", hostname);
		collisionSet.addTag("artifact_version", m_artifactVersion);
		collisionSet.addTag("deployment_id", m_deploymentId);
		collisionSet.addDataPoint(new LongDataPoint(System.currentTimeMillis(), collisions.getAndSet(0)));

		permitSet.addTag("artifact_version", m_artifactVersion);
		permitSet.addTag("deployment_id", m_deploymentId);
		permitSet.addTag("host", hostname);
		permitSet.addDataPoint(new LongDataPoint(System.currentTimeMillis(), this.getAvailableThreads()));

		queueLengthSet.addTag("artifact_version", m_artifactVersion);
		queueLengthSet.addTag("deployment_id", m_deploymentId);
		queueLengthSet.addTag("host", hostname);
		queueLengthSet.addDataPoint(new LongDataPoint(System.currentTimeMillis(), this.getQueryWaitingCount()));

		List<DataPointSet> ret = new ArrayList<>();
		ret.add(collisionSet);
		ret.add(permitSet);
		ret.add(queueLengthSet);

		return ret;
	}
}