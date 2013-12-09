/*
 * Copyright 2013 Proofpoint Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kairosdb.core.carbon;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datapoints.DoubleDataPoint;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.exception.KairosDBException;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 10/4/13
 Time: 3:52 PM
 To change this template use File | Settings | File Templates.
 */
public class CarbonPickleServerTest
{
	private final static int CARBON_PORT = 2004;

	private KairosDatastore m_datastore;
	private CarbonPickleServer m_server;
	private CarbonClient m_client;

	@Before
	public void setupDatastore() throws KairosDBException, IOException
	{
		m_datastore = mock(KairosDatastore.class);
		HostTagParser hostTagParser = new HostTagParser(
				"[^.]*\\.([^.]*)\\..*",
				"$1",
				"([^.]*)\\.[^.]*\\.(.*)",
				"$1.$2");

		m_server = new CarbonPickleServer(m_datastore, hostTagParser);
		m_server.start();

		m_client = new CarbonClient("127.0.0.1", CARBON_PORT);
	}

	@After
	public void shutdown() throws IOException
	{
		m_server.stop();
		m_client.close();
	}

	@Test
	public void test_putDataPoints_longValue() throws DatastoreException, InterruptedException, IOException
	{
		long now = System.currentTimeMillis() / 1000;

		m_client.sendPickle("test.host_name.metric_name", now, 1234);

		DataPointSet dps = new DataPointSet("test.metric_name");
		dps.addTag("host", "host_name");
		dps.addDataPoint(new LongDataPoint(now * 1000, 1234));

		verify(m_datastore, timeout(5000).times(1)).putDataPoints(dps);
	}

	@Test
	public void test_putDataPoints_doubleValue() throws DatastoreException, InterruptedException, IOException
	{
		long now = System.currentTimeMillis() / 1000;

		m_client.sendPickle("test.host_name.metric_name", now, 12.34);

		DataPointSet dps = new DataPointSet("test.metric_name");
		dps.addTag("host", "host_name");
		dps.addDataPoint(new DoubleDataPoint(now * 1000, 12.34));

		verify(m_datastore, timeout(5000).times(1)).putDataPoints(dps);
	}
}
