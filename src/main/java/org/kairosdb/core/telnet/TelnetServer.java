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
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.Delimiters;
import org.jboss.netty.handler.codec.string.StringEncoder;
import org.kairosdb.core.KairosDBService;
import org.kairosdb.core.exception.KariosDBException;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

public class TelnetServer extends SimpleChannelUpstreamHandler implements ChannelPipelineFactory,
		KairosDBService
{
	private int m_port;
	private CommandProvider m_commands;
	private UnknownCommand m_unknownCommand;

	@Inject
	public TelnetServer(@Named("kairosdb.telnetserver.port")int port,
			CommandProvider commandProvider)
	{
		m_commands = commandProvider;
		m_port = port;
	}


	@Override
	public ChannelPipeline getPipeline() throws Exception
	{
		ChannelPipeline pipeline = Channels.pipeline();

		// Add the text line codec combination first,
		DelimiterBasedFrameDecoder frameDecoder = new DelimiterBasedFrameDecoder(
				1024, Delimiters.lineDelimiter());
		pipeline.addLast("framer", frameDecoder);
		pipeline.addLast("decoder", new WordSplitter());
		pipeline.addLast("encoder", new StringEncoder());

		// and then business logic.
		pipeline.addLast("handler", this);

		return pipeline;
	}

	@Override
	public void messageReceived(final ChannelHandlerContext ctx,
			final MessageEvent msgevent) {
		try
		{
			final Object message = msgevent.getMessage();
			if (message instanceof String[])
			{
				String[] command = (String[])message;
				TelnetCommand telnetCommand = m_commands.getCommand(command[0]);
				if (telnetCommand == null)
					telnetCommand = m_unknownCommand;

				telnetCommand.execute(msgevent.getChannel(), command);
			}
			else
			{
				//TODO
				/*logError(msgevent.getChannel(), "Unexpected message type "
						+ message.getClass() + ": " + message);
				exceptions_caught.incrementAndGet();*/
			}
		}
		catch (Exception e)
		{
			Object pretty_message = msgevent.getMessage();
			if (pretty_message instanceof String[])
			{
				pretty_message = Arrays.toString((String[]) pretty_message);
			}
			//TODO
			/*logError(msgevent.getChannel(), "Unexpected exception caught"
					+ " while serving " + pretty_message, e);
			exceptions_caught.incrementAndGet();*/
		}
	}

	@Override
	public void start() throws KariosDBException
	{
		// Configure the server.
		ServerBootstrap bootstrap = new ServerBootstrap(
				new NioServerSocketChannelFactory(
						Executors.newCachedThreadPool(),
						Executors.newCachedThreadPool()));

		// Configure the pipeline factory.
		bootstrap.setPipelineFactory(this);
		bootstrap.setOption("child.tcpNoDelay", true);
		bootstrap.setOption("child.keepAlive", true);
		bootstrap.setOption("reuseAddress", true);

		// Bind and start to accept incoming connections.
		bootstrap.bind(new InetSocketAddress(m_port));
	}

	@Override
	public void stop()
	{

	}
}
