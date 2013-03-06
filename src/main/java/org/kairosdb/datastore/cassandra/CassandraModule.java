package org.kairosdb.datastore.cassandra;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import org.kairosdb.core.datastore.Datastore;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 2/8/13
 Time: 7:45 PM
 To change this template use File | Settings | File Templates.
 */
public class CassandraModule extends AbstractModule
{
	@Override
	protected void configure()
	{
		bind(Datastore.class).to(CassandraDatastore.class).in(Scopes.SINGLETON);
	}
}
