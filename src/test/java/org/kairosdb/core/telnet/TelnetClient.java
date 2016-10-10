/*
 * Copyright 2016 KairosDB Authors
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


import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 10/7/13
 Time: 11:51 AM
 To change this template use File | Settings | File Templates.
 */
public class TelnetClient implements Closeable
{
	private Socket m_socket;
	private PrintWriter m_writer;

	public TelnetClient(String host, int port) throws IOException
	{
		m_socket = new Socket(host, port);

		m_writer = new PrintWriter(m_socket.getOutputStream());
	}


	public void sendText(String msg)
	{
		m_writer.println(msg);
		m_writer.flush();
	}

	@Override
	public void close() throws IOException
	{
		m_socket.close();
	}
}
