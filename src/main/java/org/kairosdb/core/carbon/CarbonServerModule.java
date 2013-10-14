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
	public static final String TAG_PARSER_PROPERTY = "kairosdb.carbon.tagparser";
	private Properties m_properties;

	public CarbonServerModule(Properties props)
	{
		m_properties = props;
	}

	@Override
	protected void configure()
	{
		logger.info("Configuring module CarbonServerModule");

		String parserClassProp = m_properties.getProperty(TAG_PARSER_PROPERTY);

		if (parserClassProp != null)
		{
			try
			{
				Class<TagParser> parserClass = (Class<TagParser>)getClass().getClassLoader().loadClass(parserClassProp);

				bind(TagParser.class).to(parserClass).in(Singleton.class);
			}
			catch (ClassNotFoundException e)
			{
				addError(e);
			}
		}
		else
		{
			addError("No classs defined for "+TAG_PARSER_PROPERTY);
		}

		bind(CarbonTextServer.class).in(Singleton.class);
		bind(CarbonPickleServer.class).in(Singleton.class);
	}
}
