package org.kairosdb.core.carbon;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.Delimiters;
import org.jboss.netty.handler.codec.string.StringEncoder;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.KairosDBService;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.core.telnet.CommandProvider;
import org.kairosdb.core.telnet.TelnetCommand;
import org.kairosdb.core.telnet.WordSplitter;
import org.kairosdb.util.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 9/30/13
 Time: 4:09 PM
 To change this template use File | Settings | File Templates.
 */
public class CarbonTextServer extends SimpleChannelUpstreamHandler implements ChannelPipelineFactory,
		KairosDBService
{
	public static final Logger logger = LoggerFactory.getLogger(CarbonTextServer.class);

	private final int m_port;
	private final KairosDatastore m_datastore;

	@Inject
	public CarbonTextServer(KairosDatastore datastore, @Named("kairosdb.carbon.text.port") int port)
	{
		m_port = port;
		m_datastore = datastore;
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
			try
			{
				String[] msgArr = (String[])message;

				//TODO: Validate data
				DataPointSet dps = new DataPointSet(msgArr[0]);

				long timestamp = Long.parseLong(msgArr[2]) * 1000; //Converting to milliseconds

				DataPoint dp;
				if (msgArr[1].contains("."))
					dp = new DataPoint(timestamp, Double.parseDouble(msgArr[1]));
				else
					dp = new DataPoint(timestamp, Long.parseLong(msgArr[1]));

				dps.addDataPoint(dp);
				dps.addTag("host", "local");
				m_datastore.putDataPoints(dps);
			}
			catch (Exception e)
			{
				logger.error("Carbon text error", e);
			}
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