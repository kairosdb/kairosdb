package org.kairosdb.core.demo;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DemoModule extends AbstractModule
{
	public static final Logger logger = LoggerFactory.getLogger(DemoModule.class);

	@Override
	protected void configure()
	{
		System.out.println("LOADING DEMO!!!!");
		logger.info("Configuring module DemoModule");

		bind(DemoServer.class).in(Singleton.class);
	}
}
