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

package org.kairosdb.core.telnet;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.exception.DatastoreException;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MyTelnetServer
{
	private int m_port;
	private Map<String, TelnetCommand> m_commands;
	private UnknownCommand m_unknownCommand;

	@Inject
	public MyTelnetServer(@Named("kairosdb.telnetserver.port")int port,
			@Named("kairosdb.telnetserver.commands")String commands,
			CommandProvider commandProvider)
	{
		m_commands = new HashMap<String, TelnetCommand>();
		m_port = port;

		String[] splitCommands = commands.split(",");

		for (String cmd : splitCommands)
		{
			TelnetCommand telnetCommand = commandProvider.getCommand(cmd);
			if (telnetCommand != null)
				m_commands.put(cmd, telnetCommand);
		}
	}

	public void run()
	{
		Thread telnetThread = new Thread(new Runnable()
				{
					public void run()
					{
						startSocket();
					}
				});

		telnetThread.start();
	}


	private void startSocket()
	{
		try
		{
			String[] type = new String[0];

			ServerSocketChannel ssocket = ServerSocketChannel.open();
			ssocket.socket().bind(new InetSocketAddress(m_port));


			SocketChannel socket;
			while ((socket = ssocket.accept()) != null)
			{
				socket.configureBlocking(true);
				ByteBuffer buffer = ByteBuffer.allocate(1024);
				buffer.clear();
				StringBuilder sb = new StringBuilder();
				List<String> command = new ArrayList<String>();

				int bytesRead;
				while ((bytesRead = socket.read(buffer)) > 0)
				{
					buffer.flip();
					//System.out.println("Read " + bytesRead);
					//System.out.println("loop");

					while (buffer.hasRemaining())
					{
						char c = (char)buffer.get();
						//System.out.print(c);
						if (c == ' ')
						{
							command.add(sb.toString());
							sb = new StringBuilder();
							continue;
						}

						if (c == '\n')
						{
							command.add(sb.toString());
							sb = new StringBuilder();

							callCommand(command.toArray(type));

							command.clear();
							continue;
						}

						sb.append(c);

					}

					//System.out.println("Clearing");
					buffer.clear();
				}

				//System.out.println("No more");
				socket.close();
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	private void callCommand(String[] command) throws DatastoreException
	{
		/*for (String cmd : command)
			System.out.print(cmd + " ");
		System.out.println();*/

		TelnetCommand telnetCommand = m_commands.get(command[0]);
		if (telnetCommand == null)
			telnetCommand = m_unknownCommand;

		telnetCommand.execute(null, command);
	}
}
