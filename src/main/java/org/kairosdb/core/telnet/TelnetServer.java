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
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.util.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Executors;

public class TelnetServer extends SimpleChannelUpstreamHandler implements ChannelPipelineFactory,
		KairosDBService
{
	public static final Logger logger = LoggerFactory.getLogger(TelnetServer.class);


	private int m_port;
	private InetAddress m_address;
	private CommandProvider m_commands;
	private ServerBootstrap m_serverBootstrap;

	public TelnetServer(@Named("kairosdb.telnetserver.port") int port,
	                    CommandProvider commandProvider)
	{
		this(port, null, commandProvider);
	}

	@Inject
	public TelnetServer(@Named("kairosdb.telnetserver.port") int port,
	                    @Named("kairosdb.telnetserver.address") String address,
	                    CommandProvider commandProvider)
	{
		m_commands = commandProvider;
		m_port = port;
		m_address = null;
        try
        {
            m_address = InetAddress.getByName(address);
        }
        catch (UnknownHostException e)
        {
			logger.error("Unknown host name " + address + ", will bind to 0.0.0.0");
        }
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
	                            final MessageEvent msgevent)
	{
		final Object message = msgevent.getMessage();
		if (message instanceof String[])
		{
			String[] command = (String[]) message;
			TelnetCommand telnetCommand = m_commands.getCommand(command[0]);
			if (telnetCommand != null)
			{
				try
				{
					telnetCommand.execute(msgevent.getChannel(), command);
				}
				catch(ValidationException e)
				{
					log("Failed to execute command: " + formatCommand(command), e);
				}
				catch (Exception e)
				{
					logger.error("", e);
				}
			}
			else
				log("Unknown command: " + command[0]);
		}
		else
		{
			log("Invalid message. Must be of type String.");
		}
	}

	private static void log(String message)
	{
		log(message, null);
	}

	private static void log(String message, Exception e)
	{
		if (logger.isDebugEnabled())
			if (e != null)
				logger.debug(message, e);
			else
				logger.debug(message);
		else
		{
			if (e instanceof ValidationException)
				message = message + " Reason: " + e.getMessage();
			logger.warn(message);
		}
	}

	@Override
	public void start() throws KairosDBException
	{
		// Configure the server.
		m_serverBootstrap = new ServerBootstrap(
				new NioServerSocketChannelFactory(
						Executors.newCachedThreadPool(),
						Executors.newCachedThreadPool()));

		// Configure the pipeline factory.
		m_serverBootstrap.setPipelineFactory(this);
		m_serverBootstrap.setOption("child.tcpNoDelay", true);
		m_serverBootstrap.setOption("child.keepAlive", true);
		m_serverBootstrap.setOption("reuseAddress", true);

		// Bind and start to accept incoming connections.
		m_serverBootstrap.bind(new InetSocketAddress(m_address, m_port));
	}

	@Override
	public void stop()
	{
		if (m_serverBootstrap != null)
			m_serverBootstrap.shutdown();
	}

	private static String formatCommand(String[] command)
	{
		StringBuilder builder = new StringBuilder();
		for (String s : command)
		{
			builder.append(s).append(" ");
		}

		return builder.toString();
	}
}
