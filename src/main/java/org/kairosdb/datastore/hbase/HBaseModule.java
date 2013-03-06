package org.kairosdb.datastore.hbase;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import org.kairosdb.core.datastore.Datastore;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 2/8/13
 Time: 7:47 PM
 To change this template use File | Settings | File Templates.
 */
public class HBaseModule extends AbstractModule
{
	@Override
	protected void configure()
	{
		bind(Datastore.class).to(HBaseDatastore.class).in(Scopes.SINGLETON);
	}
}
