/*
 * Copyright 2016 KairosDB Authors
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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.Executors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class TelnetServer extends SimpleChannelUpstreamHandler implements ChannelPipelineFactory,
		KairosDBService
{
	private static final Logger logger = LoggerFactory.getLogger(TelnetServer.class);

	private final int port;
	private final CommandProvider commandProvider;
	private final int maxCommandLength;

	private InetAddress address;
	private ServerBootstrap serverBootstrap;

	public TelnetServer(int port,
			int maxCommandLength,
			CommandProvider commandProvider)
			throws UnknownHostException
	{
		this(null, port, maxCommandLength, commandProvider);
	}

	@Inject
	public TelnetServer(@Named("kairosdb.telnetserver.address") String address,
			@Named("kairosdb.telnetserver.port") int port,
			@Named("kairosdb.telnetserver.max_command_size") int maxCommandLength,
			CommandProvider commandProvider)
			throws UnknownHostException
	{
		checkArgument(maxCommandLength > 0, "command length must be greater than zero");

		this.port = port;
		this.maxCommandLength = maxCommandLength;
		this.commandProvider = checkNotNull(commandProvider);
		this.address = InetAddress.getByName(address);
	}

	@Override
	public ChannelPipeline getPipeline() throws Exception
	{
		ChannelPipeline pipeline = Channels.pipeline();

		// Add the text line codec combination first,
		DelimiterBasedFrameDecoder frameDecoder = new DelimiterBasedFrameDecoder(
				maxCommandLength, Delimiters.lineDelimiter());
		pipeline.addLast("framer", frameDecoder);
		pipeline.addLast("decoder", new WordSplitter());
		pipeline.addLast("encoder", new StringEncoder());

		// and then business logic.
		pipeline.addLast("handler", this);

		return pipeline;
	}

	private String formatMessage(List<String> msg)
	{
		StringBuilder sb = new StringBuilder();
		for (String s : msg)
			sb.append(s).append(" ");

		return (sb.toString());
	}

	@Override
	public void messageReceived(final ChannelHandlerContext ctx,
	                            final MessageEvent msgevent)
	{
		final Object message = msgevent.getMessage();
		if (message instanceof List)
		{
			@SuppressWarnings("unchecked")
			List<String> command = (List<String>) message;

			String cmd = "";
			if (command.size() >= 1)
				cmd = command.get(0);

			TelnetCommand telnetCommand = commandProvider.getCommand(cmd);
			if (telnetCommand != null)
			{
				try
				{
					telnetCommand.execute(msgevent.getChannel(), command);
				}
				catch (Exception e)
				{
					log("Message: '" + formatMessage(command) + "'", ctx);
					log("Failed to execute command: " + formatMessage(command) + " Reason: " + e.getMessage(), ctx, e);
				}
			}
			else
			{
				log("Message: '" + formatMessage(command) + "'", ctx);
				log("Unknown command: '" + cmd + "'", ctx);
			}
		}
		else
		{
			log("Message: '" + message.toString() + "'", ctx);
			log("Invalid message. Must be of type String.", ctx);
		}
	}

	private static void log(String message, ChannelHandlerContext ctx)
	{
		log(message, ctx, null);
	}

	private static void log(String message, ChannelHandlerContext ctx, Exception e)
	{
		message += " From: " + ((InetSocketAddress) ctx.getChannel().getRemoteAddress()).getAddress().getHostAddress();
		if (logger.isDebugEnabled())
			if (e != null)
				logger.debug(message, e);
			else
				logger.debug(message);
		else
		{
			logger.warn(message);
		}
	}

	@Override
	public void start() throws KairosDBException
	{
		// Configure the server.
		serverBootstrap = new ServerBootstrap(
				new NioServerSocketChannelFactory(
						Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("telnet-boss-%d").build()),
						Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("telnet-worker-%d").build())));

		// Configure the pipeline factory.
		serverBootstrap.setPipelineFactory(this);
		serverBootstrap.setOption("child.tcpNoDelay", true);
		serverBootstrap.setOption("child.keepAlive", true);
		serverBootstrap.setOption("reuseAddress", true);

		// Bind and start to accept incoming connections.
		serverBootstrap.bind(new InetSocketAddress(address, port));
	}

	public InetAddress getAddress()
	{
		return address;
	}

	@Override
	public void stop()
	{
		if (serverBootstrap != null)
			serverBootstrap.shutdown();
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception
	{
		logger.error("Error in TelnetServer", e.getCause());
	}
}
