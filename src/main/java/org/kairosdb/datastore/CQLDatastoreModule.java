package org.kairosdb.datastore;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.datastore.cassandra.CassandraDatastore;

public class CQLDatastoreModule extends AbstractModule
{
	@Override
	protected void configure()
	{
		bind(Datastore.class).to(CQLDatastore.class).in(Scopes.SINGLETON);
	}
}
