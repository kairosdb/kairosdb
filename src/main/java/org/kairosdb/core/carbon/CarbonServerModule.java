package org.kairosdb.core.carbon;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 9/30/13
 Time: 4:07 PM
 To change this template use File | Settings | File Templates.
 */
public class CarbonServerModule extends AbstractModule
{
	public static final Logger logger = LoggerFactory.getLogger(CarbonServerModule.class);
	private Properties m_properties;

	public CarbonServerModule(Properties props)
	{
		m_properties = props;
	}

	@Override
	protected void configure()
	{
		logger.info("Configuring module CarbonServerModule");


		bind(CarbonTextServer.class).in(Singleton.class);
		/*bind(TelnetServer.class).in(Singleton.class);
		bind(PutCommand.class).in(Singleton.class);
		bind(VersionCommand.class).in(Singleton.class);
		bind(CommandProvider.class).to(GuiceCommandProvider.class);*/
	}
}
