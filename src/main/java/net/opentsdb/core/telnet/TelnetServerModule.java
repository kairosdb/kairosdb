package net.opentsdb.core.telnet;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 2/8/13
 Time: 5:39 PM
 To change this template use File | Settings | File Templates.
 */
public class TelnetServerModule extends AbstractModule
{
	public static final Logger logger = LoggerFactory.getLogger(TelnetServerModule.class);

	private Properties m_props;


	public TelnetServerModule(Properties props)
	{
		m_props = props;
	}

	@Override
	protected void configure()
	{
		logger.info("Configuring module TelnetServerModule");

		bind(TelnetServer.class).in(Singleton.class);
		bind(TelnetCommand.class).annotatedWith(Names.named("put")).to(PutCommand.class);
		bind(TelnetCommand.class).annotatedWith(Names.named("version")).to(VersionCommand.class);
		bind(CommandProvider.class).to(GuiceCommandProvider.class);
	}
}
