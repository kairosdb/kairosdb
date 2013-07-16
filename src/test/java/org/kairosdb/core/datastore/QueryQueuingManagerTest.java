//
//  QueryQueuingManagerTest.java
//
// Copyright 2013, Proofpoint Inc. All rights reserved.
//        
package org.kairosdb.core.datastore;

import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.DataPointSet;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class QueryQueuingManagerTest
{
	private AtomicInteger runningCount;

	@Before
	public void setup()
	{
		runningCount = new AtomicInteger();
	}

	@Test(timeout = 3000)
	public void test_onePermit() throws InterruptedException
	{
		QueryQueuingManager manager = new QueryQueuingManager(1, "hostname");

		Query query1 = new Query(manager, "1", 5);
		Query query2 = new Query(manager, "2", 5);
		Query query3 = new Query(manager, "3", 5);
		Query query4 = new Query(manager, "4", 5);
		Query query5 = new Query(manager, "5", 5);

		query1.start();
		query2.start();
		query3.start();
		query4.start();
		query5.start();

		query1.join();
		query2.join();
		query3.join();
		query4.join();
		query5.join();

		assertThat(query1.didRun, equalTo(true));
		assertThat(query2.didRun, equalTo(true));
		assertThat(query3.didRun, equalTo(true));
		assertThat(query4.didRun, equalTo(true));
		assertThat(query5.didRun, equalTo(true));

		assertThat(query1.queriesWatiting, equalTo(4L));
		assertThat(query2.queriesWatiting, equalTo(3L));
		assertThat(query3.queriesWatiting, equalTo(2L));
		assertThat(query4.queriesWatiting, equalTo(1L));
		assertThat(query5.queriesWatiting, equalTo(0L));
	}

	@Test(timeout = 3000)
	public void test_onePermitSameHash() throws InterruptedException
	{
		QueryQueuingManager manager = new QueryQueuingManager(3, "hostname");

		Query query1 = new Query(manager, "1", 5);
		Query query2 = new Query(manager, "1", 5);
		Query query3 = new Query(manager, "1", 5);
		Query query4 = new Query(manager, "1", 5);
		Query query5 = new Query(manager, "1", 5);

		query1.start();
		query2.start();
		query3.start();
		query4.start();
		query5.start();

		query1.join();
		query2.join();
		query3.join();
		query4.join();
		query5.join();

		assertThat(query1.didRun, equalTo(true));
		assertThat(query2.didRun, equalTo(true));
		assertThat(query3.didRun, equalTo(true));
		assertThat(query4.didRun, equalTo(true));
		assertThat(query5.didRun, equalTo(true));

		//Number of collisions
		assertThat(manager.getMetrics(System.currentTimeMillis()).get(1).getDataPoints().get(0).getLongValue(), equalTo(4L));
	}

	@Test(timeout = 3000)
	public void test_EnoughPermitsDifferentHashes() throws InterruptedException
	{
		QueryQueuingManager manager = new QueryQueuingManager(3, "hostname");

		Query query1 = new Query(manager, "1", 3);
		Query query2 = new Query(manager, "2", 3);
		Query query3 = new Query(manager, "3", 3);

		query1.start();
		query2.start();
		query3.start();

		query1.join();
		query2.join();
		query3.join();

		assertThat(query1.didRun, equalTo(true));
		assertThat(query2.didRun, equalTo(true));
		assertThat(query3.didRun, equalTo(true));

		List<DataPointSet> metrics = manager.getMetrics(System.currentTimeMillis());
		assertThat(metrics.get(0).getDataPoints().get(0).getLongValue(), equalTo(0L));
		assertThat(metrics.get(1).getDataPoints().get(0).getLongValue(), equalTo(0L));
	}

	private class Query extends Thread
	{
		private QueryQueuingManager manager;
		private String hash;
		private int waitCount;
		private boolean didRun = false;
		private long queriesWatiting;

		private Query(QueryQueuingManager manager, String hash, int waitCount)
		{
			this.manager = manager;
			this.hash = hash;
			this.waitCount = waitCount;
		}

		@Override
		public void run()
		{
			try
			{
				runningCount.incrementAndGet();
				manager.waitForTimeToRun(hash);
				while(runningCount.get() < waitCount)
				{
					Thread.sleep(100);
				}
				queriesWatiting = manager.getQueryWaitingCount();
			}
			catch (InterruptedException e)
			{
				assertFalse("InterruptedException", false);
			}

			didRun = true;
			manager.done(hash);
		}
	}
}
