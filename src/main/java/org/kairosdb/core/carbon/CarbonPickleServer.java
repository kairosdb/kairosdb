package org.kairosdb.core.carbon;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.kairosdb.core.KairosDBService;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.exception.KairosDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
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

	private final int m_port;
	private final KairosDatastore m_datastore;
	private final TagParser m_tagParser;

	@Inject
	public CarbonPickleServer(KairosDatastore datastore,
			TagParser tagParser, @Named("kairosdb.carbon.pickle.port") int port)
	{
		m_port = port;
		m_datastore = datastore;
		m_tagParser = tagParser;
	}

	@Override
	public ChannelPipeline getPipeline() throws Exception
	{
		ChannelPipeline pipeline = Channels.pipeline();

		LengthFieldBasedFrameDecoder frameDecoder = new LengthFieldBasedFrameDecoder(
				0, 0, 8);

		pipeline.addLast("framer", frameDecoder);

		pipeline.addLast("handler", this);

		return (pipeline);
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
}
