//
//  QueryQueuingManagerTest.java
//
// Copyright 2013, Proofpoint Inc. All rights reserved.
//        
package org.kairosdb.core.datastore;

import org.junit.Test;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class QueryQueuingManagerTest
{
	@Test(timeout = 3000)
	public void test_onePermit() throws InterruptedException
	{
		QueryQueuingManager manager = new QueryQueuingManager(1, "hostname");

		Query query1 = new Query(manager, "1");
		Query query2 = new Query(manager, "2");
		Query query3 = new Query(manager, "3");
		Query query4 = new Query(manager, "4");
		Query query5 = new Query(manager, "5");

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

		// Assume that at least 1 had to wait
		long queryWaits = manager.getMetrics(System.currentTimeMillis()).get(0).getDataPoints().get(0).getLongValue();
		assertThat(queryWaits, greaterThan(1L));

		System.out.println(queryWaits);
	}

	@Test(timeout = 3000)
	public void test_onePermitSameHash() throws InterruptedException
	{
		QueryQueuingManager manager = new QueryQueuingManager(3, "hostname");

		Query query1 = new Query(manager, "1");
		Query query2 = new Query(manager, "1");
		Query query3 = new Query(manager, "1");
		Query query4 = new Query(manager, "1");
		Query query5 = new Query(manager, "1");

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

		assertThat(manager.getMetrics(System.currentTimeMillis()).get(0).getDataPoints().get(0).getLongValue(), greaterThan(1L));
	}

	@Test(timeout = 3000)
	public void test_EnoughPermitsDifferentHashes() throws InterruptedException
	{
		QueryQueuingManager manager = new QueryQueuingManager(3, "hostname");

		Query query1 = new Query(manager, "1");
		Query query2 = new Query(manager, "2");
		Query query3 = new Query(manager, "3");

		query1.start();
		query2.start();
		query3.start();

		query1.join();
		query2.join();
		query3.join();

		assertThat(query1.didRun, equalTo(true));
		assertThat(query2.didRun, equalTo(true));
		assertThat(query3.didRun, equalTo(true));

		assertThat(manager.getMetrics(System.currentTimeMillis()).get(0).getDataPoints().get(0).getLongValue(), equalTo(0L));
	}

	private class Query extends Thread
	{
		private QueryQueuingManager manager;
		private String hash;
		private boolean didRun = false;

		private Query(QueryQueuingManager manager, String hash)
		{
			this.manager = manager;
			this.hash = hash;
		}

		@Override
		public void run()
		{
			try
			{
				manager.waitForTimeToRun(hash);
				Thread.sleep(100);
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
