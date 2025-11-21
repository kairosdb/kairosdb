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
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;
import org.kairosdb.core.KairosDBService;
import org.kairosdb.core.exception.KairosDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@ChannelHandler.Sharable
public class TelnetServer extends ChannelInboundHandlerAdapter implements KairosDBService
{
	private static final Logger logger = LoggerFactory.getLogger(TelnetServer.class);

	private final int port;
	private final CommandProvider commandProvider;
	private final int maxCommandLength;

	private final InetAddress address;
	private ServerBootstrap serverBootstrap;
	private NioEventLoopGroup bossGroup;
	private NioEventLoopGroup workerGroup;
	private Channel serverChannel;

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
		this.commandProvider = requireNonNull(commandProvider);
		this.address = InetAddress.getByName(address);
	}

	private ChannelInitializer<SocketChannel> createChannelInitializer()
	{
		return new ChannelInitializer<SocketChannel>()
		{
			@Override
			protected void initChannel(SocketChannel ch) throws Exception
			{
				ChannelPipeline pipeline = ch.pipeline();

				// Add the text line codec combination first,
				DelimiterBasedFrameDecoder frameDecoder = new DelimiterBasedFrameDecoder(
						maxCommandLength, Delimiters.lineDelimiter());
				pipeline.addLast("framer", frameDecoder);
				pipeline.addLast("decoder", new WordSplitter());
				pipeline.addLast("encoder", new StringEncoder(CharsetUtil.UTF_8));

				// and then business logic.
				pipeline.addLast("handler", TelnetServer.this);
			}
		};
	}

	private String formatMessage(List<String> msg)
	{
		StringBuilder sb = new StringBuilder();
		for (String s : msg)
			sb.append(s).append(" ");

		return (sb.toString());
	}

	@Override
	public void channelRead(final ChannelHandlerContext ctx,
	                        final Object message)
	{
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
					telnetCommand.execute(ctx.channel(), command);
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
		message += " From: " + ((InetSocketAddress) ctx.channel().remoteAddress()).getAddress().getHostAddress();
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
		bossGroup = new NioEventLoopGroup(1, new ThreadFactoryBuilder().setNameFormat("telnet-boss-%d").build());
		workerGroup = new NioEventLoopGroup(0, new ThreadFactoryBuilder().setNameFormat("telnet-worker-%d").build());

		serverBootstrap = new ServerBootstrap();
		serverBootstrap.group(bossGroup, workerGroup)
				.channel(NioServerSocketChannel.class)
				.childHandler(createChannelInitializer())
				.option(ChannelOption.SO_REUSEADDR, true)
				.childOption(ChannelOption.TCP_NODELAY, true)
				.childOption(ChannelOption.SO_KEEPALIVE, true);

		// Bind and start to accept incoming connections.
		try
		{
			serverChannel = serverBootstrap.bind(new InetSocketAddress(address, port)).sync().channel();
		}
		catch (InterruptedException e)
		{
			Thread.currentThread().interrupt();
			throw new KairosDBException("Failed to start telnet server", e);
		}
	}

	public InetAddress getAddress()
	{
		return address;
	}

	@Override
	public void stop()
	{
		if (serverChannel != null)
		{
			try
			{
				serverChannel.close().sync();
			}
			catch (InterruptedException e)
			{
				Thread.currentThread().interrupt();
				logger.warn("Interrupted while closing telnet server channel", e);
			}
		}

		if (workerGroup != null)
			workerGroup.shutdownGracefully();

		if (bossGroup != null)
			bossGroup.shutdownGracefully();
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
	{
		logger.error("Error in TelnetServer", cause);
		ctx.close();
	}
}
