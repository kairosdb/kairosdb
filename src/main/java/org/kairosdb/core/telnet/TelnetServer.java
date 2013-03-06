// KairosDB2
// Copyright (C) 2013 Proofpoint, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>

package org.kairosdb.core.telnet;

import com.google.inject.Inject;
import org.kairosdb.core.KairosDBService;
import org.kairosdb.core.exception.TsdbException;
import org.jboss.netty.channel.*;
import com.google.inject.name.Named;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.Delimiters;
import org.jboss.netty.handler.codec.string.StringEncoder;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

public class TelnetServer extends SimpleChannelUpstreamHandler implements ChannelPipelineFactory,
		KairosDBService
{
	private int m_port;
	private Map<String, TelnetCommand> m_commands;
	private UnknownCommand m_unknownCommand;

	@Inject
	public TelnetServer(@Named("kairosdb.telnetserver.port")int port,
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
		try {
			final Object message = msgevent.getMessage();
			if (message instanceof String[])
			{
				String[] command = (String[])message;
				TelnetCommand telnetCommand = m_commands.get(command[0]);
				if (telnetCommand == null)
					telnetCommand = m_unknownCommand;

				telnetCommand.execute(msgevent.getChannel(), command);
			} else {
				//TODO
				/*logError(msgevent.getChannel(), "Unexpected message type "
						+ message.getClass() + ": " + message);
				exceptions_caught.incrementAndGet();*/
			}
		} catch (Exception e) {
			Object pretty_message = msgevent.getMessage();
			if (pretty_message instanceof String[]) {
				pretty_message = Arrays.toString((String[]) pretty_message);
			}
			//TODO
			/*logError(msgevent.getChannel(), "Unexpected exception caught"
					+ " while serving " + pretty_message, e);
			exceptions_caught.incrementAndGet();*/
		}
	}

	@Override
	public void start() throws TsdbException
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
