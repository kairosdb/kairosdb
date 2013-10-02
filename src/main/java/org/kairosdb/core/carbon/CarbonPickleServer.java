package org.kairosdb.core.carbon;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.KairosDBService;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.exception.KairosDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.Executors;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 10/1/13
 Time: 4:43 PM
 To change this template use File | Settings | File Templates.
 */
public class CarbonPickleServer extends SimpleChannelUpstreamHandler implements ChannelPipelineFactory,
		KairosDBService
{
	public static final Logger logger = LoggerFactory.getLogger(CarbonPickleServer.class);

	@Inject
	@Named("kairosdb.carbon.pickle.port")
	private int m_port = 2004;

	@Inject
	@Named("kairosdb.carbon.pickle.max_size")
	private int m_maxSize = 2048;

	private final KairosDatastore m_datastore;
	private final TagParser m_tagParser;

	@Inject
	public CarbonPickleServer(KairosDatastore datastore, TagParser tagParser)
	{
		m_datastore = datastore;
		m_tagParser = tagParser;
	}

	@Override
	public ChannelPipeline getPipeline() throws Exception
	{
		ChannelPipeline pipeline = Channels.pipeline();

		LengthFieldBasedFrameDecoder frameDecoder = new LengthFieldBasedFrameDecoder(
				m_maxSize, 0, 4, 0, 4);

		pipeline.addLast("framer", frameDecoder);
		pipeline.addLast("decoder", new PickleDecoder());

		pipeline.addLast("handler", this);

		return (pipeline);
	}

	@Override
	public void messageReceived(final ChannelHandlerContext ctx,
			final MessageEvent msgevent)
	{
		//System.out.println("I GOT ONE!!!!");

		if (msgevent.getMessage() instanceof ArrayList)
		{
			for (Object o : (ArrayList) msgevent.getMessage())
			{
				Object[] dp = (Object[])o;

				String name = (String)dp[0];
				long time = ((Double)((Object[])dp[1])[0]).longValue();
				Object value = ((Object[])dp[1])[1];

				if (logger.isDebugEnabled())
					logger.debug(name+" "+time+" "+value);

				DataPointSet dps = m_tagParser.parseMetricName(name);
				if (dps == null)
					continue;

				time *= 1000;  //Convert to milliseconds
				if (value instanceof Long)
					dps.addDataPoint(new DataPoint(time, ((Long) value).longValue()));
				else if (value instanceof Integer)
					dps.addDataPoint(new DataPoint(time, ((Integer) value).intValue()));
				else
					dps.addDataPoint(new DataPoint(time, ((Double) value).doubleValue()));

				try
				{
					m_datastore.putDataPoints(dps);
				}
				catch (DatastoreException e)
				{
					e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
				}
			}
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
		//To change body of implemented methods use File | Settings | File Templates.
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception
	{
		logger.error("Error processing pickle", e.getCause());
	}
}
