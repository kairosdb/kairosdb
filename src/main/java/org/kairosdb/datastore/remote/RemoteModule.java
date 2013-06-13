package org.kairosdb.datastore.remote;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import org.kairosdb.core.datastore.Datastore;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 5/31/13
 Time: 9:03 AM
 To change this template use File | Settings | File Templates.
 */
public class RemoteModule extends AbstractModule
{
	@Override
	protected void configure()
	{
		bind(Datastore.class).to(RemoteDatastore.class).in(Scopes.SINGLETON);
		bind(RemoteDatastore.class).in(Scopes.SINGLETON);
		bind(RemoteSendJob.class).in(Scopes.SINGLETON);
	}
}
