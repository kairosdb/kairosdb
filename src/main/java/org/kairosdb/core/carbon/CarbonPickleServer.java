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
import org.kairosdb.core.carbon.pickle.PickleMetric;
import org.kairosdb.core.datapoints.*;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.exception.KairosDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
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
	private LongDataPointFactory m_longDataPointFactory = new LongDataPointFactoryImpl();

	@Inject
	private DoubleDataPointFactory m_doubleDataPointFactory = new DoubleDataPointFactoryImpl();

	@Inject
	@Named("kairosdb.carbon.pickle.port")
	private int m_port = 2004;

	@Inject
	@Named("kairosdb.carbon.pickle.max_size")
	private int m_maxSize = 2048;

	private final KairosDatastore m_datastore;
	private final TagParser m_tagParser;
	private ServerBootstrap m_serverBootstrap;

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
		if (msgevent.getMessage() instanceof List)
		{
			for (Object o : (List) msgevent.getMessage())
			{
				//todo verify cast
				PickleMetric metric = (PickleMetric)o;

				DataPointSet dps = m_tagParser.parseMetricName(metric.getPath());
				if (dps == null)
					continue;

				//validate dps has at least one tag
				if (dps.getTags().size() == 0)
				{
					logger.warn("Metric "+metric.getPath()+" is missing a tag");
					return;
				}

				String metricName = dps.getName();
				SortedMap<String, String> tags = dps.getTags();

				long time = metric.getTime();
				DataPoint dataPoint;
				time *= 1000;  //Convert to milliseconds
				if (metric.isLongValue())
					dataPoint = m_longDataPointFactory.createDataPoint(time, metric.getLongValue());
				else
					dataPoint = m_doubleDataPointFactory.createDataPoint(time, metric.getDoubleValue());

				try
				{
					m_datastore.putDataPoint(metricName, tags, dataPoint);
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
		m_serverBootstrap.bind(new InetSocketAddress(m_port));
	}

	@Override
	public void stop()
	{
		if (m_serverBootstrap != null)
			m_serverBootstrap.shutdown();
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception
	{
		logger.error("Error processing pickle", e.getCause());
	}
}
