package org.kairosdb.datastore.remote;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import org.kairosdb.util.DiskUtils;
import org.kairosdb.util.DiskUtilsImpl;

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
		bind(RemoteDatastoreHealthCheck.class).in(Scopes.SINGLETON);
		bind(DiskUtils.class).to(DiskUtilsImpl.class);
		bind(RemoteHost.class).to(RemoteHostImpl.class);
	}
}
