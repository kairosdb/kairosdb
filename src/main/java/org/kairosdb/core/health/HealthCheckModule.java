package org.kairosdb.core.health;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;

import javax.inject.Singleton;

public class HealthCheckModule extends AbstractModule
{
	@Override
	protected void configure()
	{
		bind(HealthCheckService.class).to(HealthCheckServiceImpl.class).in(Singleton.class);
		bind(ThreadDeadlockHealthStatus.class).in(Singleton.class);
		bind(DatastoreQueryHealthCheck.class).in(Singleton.class);

		// Bind REST resource
		bind(HealthCheckResource.class).in(Scopes.SINGLETON);
	}
}
