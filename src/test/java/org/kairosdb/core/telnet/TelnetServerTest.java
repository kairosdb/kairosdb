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

package org.kairosdb.core.telnet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.carbon.CarbonClient;
import org.kairosdb.core.carbon.CarbonPickleServer;
import org.kairosdb.core.carbon.HostTagParser;
import org.kairosdb.core.datapoints.DoubleDataPointFactoryImpl;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;
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
 Date: 10/7/13
 Time: 11:30 AM
 To change this template use File | Settings | File Templates.
 */
public class TelnetServerTest
{
	private static final int TELNET_PORT = 4244;
	private KairosDatastore m_datastore;
	private TelnetServer m_server;
	private TelnetClient m_client;


	@Before
	public void setupDatastore() throws KairosDBException, IOException
	{
		m_datastore = mock(KairosDatastore.class);
		TestCommandProvider commandProvider = new TestCommandProvider();
		commandProvider.putCommand("put", new PutCommand(m_datastore, "localhost",
				new LongDataPointFactoryImpl(), new DoubleDataPointFactoryImpl()));

		m_server = new TelnetServer(TELNET_PORT, commandProvider);
		m_server.start();

		m_client = new TelnetClient("127.0.0.1", TELNET_PORT);
	}

	@After
	public void shutdown() throws IOException
	{
		m_server.stop();
		m_client.close();
	}

	@Test
	public void test_extraSpaceAfterValue() throws DatastoreException
	{
		long now = System.currentTimeMillis() / 1000;

		m_client.sendText("put test.metric "+now+" 123  host=test_host");

		DataPointSet dps = new DataPointSet("test.metric");
		dps.addTag("host", "test_host");
		dps.addDataPoint(new LongDataPoint(now * 1000, 123));

		verify(m_datastore, timeout(5000).times(1)).putDataPoints(dps);
	}

	@Test
	public void test_extraSpaceAfterMetric() throws DatastoreException
	{
		long now = System.currentTimeMillis() / 1000;

		m_client.sendText("put test.metric  "+now+" 123 host=test_host");

		DataPointSet dps = new DataPointSet("test.metric");
		dps.addTag("host", "test_host");
		dps.addDataPoint(new LongDataPoint(now * 1000, 123));

		verify(m_datastore, timeout(5000).times(1)).putDataPoints(dps);
	}

	@Test
	public void test_extraSpaceAfterTime() throws DatastoreException
	{
		long now = System.currentTimeMillis() / 1000;

		m_client.sendText("put test.metric "+now+"  123 host=test_host");

		DataPointSet dps = new DataPointSet("test.metric");
		dps.addTag("host", "test_host");
		dps.addDataPoint(new LongDataPoint(now * 1000, 123));

		verify(m_datastore, timeout(5000).times(1)).putDataPoints(dps);
	}

	@Test
	public void test_tripleSpaceAfterTime() throws DatastoreException
	{
		long now = System.currentTimeMillis() / 1000;

		m_client.sendText("put test.metric "+now+"   123 host=test_host");

		DataPointSet dps = new DataPointSet("test.metric");
		dps.addTag("host", "test_host");
		dps.addDataPoint(new LongDataPoint(now * 1000, 123));

		verify(m_datastore, timeout(5000).times(1)).putDataPoints(dps);
	}
}
