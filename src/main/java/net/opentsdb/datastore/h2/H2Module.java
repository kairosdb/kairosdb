package net.opentsdb.datastore.h2;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import net.opentsdb.core.datastore.Datastore;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 2/8/13
 Time: 7:46 PM
 To change this template use File | Settings | File Templates.
 */
public class H2Module extends AbstractModule
{
	@Override
	protected void configure()
	{
		bind(Datastore.class).to(H2Datastore.class).in(Scopes.SINGLETON);
	}
}
