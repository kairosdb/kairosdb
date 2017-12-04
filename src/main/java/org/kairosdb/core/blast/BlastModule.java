package org.kairosdb.core.blast;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 Created by bhawkins on 5/16/14.
 */
public class BlastModule extends AbstractModule
{
	public static final Logger logger = LoggerFactory.getLogger(BlastModule.class);

	@Override
	protected void configure()
	{
		System.out.println("LOADING BLAST!!!!");
		logger.info("Configuring module BlastModule");

		bind(BlastServer.class).in(Singleton.class);
	}
}
