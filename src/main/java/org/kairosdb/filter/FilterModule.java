package org.kairosdb.filter;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterModule extends AbstractModule
{
	public static final Logger logger = LoggerFactory.getLogger(FilterModule.class);

	@Override
	protected void configure()
	{
		bind(FilterPlugin.class).in(Singleton.class);
	}
}
