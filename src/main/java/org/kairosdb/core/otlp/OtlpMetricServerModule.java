package org.kairosdb.core.otlp;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import org.kairosdb.core.KairosRootConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OtlpMetricServerModule extends AbstractModule
{
	public static final Logger logger = LoggerFactory.getLogger(OtlpMetricServerModule.class);

	public OtlpMetricServerModule(KairosRootConfig config)
	{

	}

	@Override
	protected void configure()
	{
		logger.info("Configuring module OtlpMetricServerModule");

		bind(OtlpMetricServer.class).in(Singleton.class);
	}
}
