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

import com.mchange.v1.util.ClosableResource;
import org.kairosdb.core.carbon.pickle.PickleMetric;
import org.kairosdb.core.carbon.pickle.Pickler;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 10/4/13
 Time: 2:44 PM
 To change this template use File | Settings | File Templates.
 */
public class CarbonClient implements Closeable
{
	private Socket m_socket;
	private PrintWriter m_writer;
	private DataOutputStream m_data;

	public CarbonClient(String host, int port) throws IOException
	{
		m_socket = new Socket(host, port);

		m_writer = new PrintWriter(m_socket.getOutputStream());
		m_data = new DataOutputStream(m_socket.getOutputStream());
	}

	private Object[] createTuple(Object o1, Object o2)
	{
		return (new Object[] { o1, o2 });
	}

	public void sendText(String metricName, long timestamp, String value)
	{
		StringBuilder sb = new StringBuilder();
		sb.append(metricName).append(" ");
		sb.append(value).append(" ");
		sb.append(timestamp);

		m_writer.println(sb.toString());
		m_writer.flush();
	}

	public void sendPickle(String metricName, long timestamp, long value) throws IOException
	{
		List list = new ArrayList<PickleMetric>();

		list.add(new PickleMetric(metricName, timestamp, value));

		sendPickleList(list);
	}

	public void sendPickle(String metricName, long timestamp, double value) throws IOException
	{
		List list = new ArrayList<PickleMetric>();

		list.add(new PickleMetric(metricName, timestamp, value));

		sendPickleList(list);
	}

	public void sendPickleList(List<PickleMetric> list) throws IOException
	{
		Pickler pickler = new Pickler();
		ByteArrayOutputStream bo = new ByteArrayOutputStream();
		pickler.writeMetrics(list, bo);
		bo.flush();
		byte[] pickleBlob = bo.toByteArray();

		m_data.writeInt(pickleBlob.length);
		m_data.write(pickleBlob);
		m_data.flush();
	}


	@Override
	public void close() throws IOException
	{
		m_socket.close();
	}
}
