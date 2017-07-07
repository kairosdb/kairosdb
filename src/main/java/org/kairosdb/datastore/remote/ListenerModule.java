package org.kairosdb.datastore.remote;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;

/**
 Created by bhawkins on 8/29/16.
 */
public class ListenerModule extends AbstractModule
{
	@Override
	protected void configure()
	{
		bind(RemoteDatastore.class).in(Scopes.SINGLETON);
		bind(RemoteSendJob.class).in(Scopes.SINGLETON);
	}
}
