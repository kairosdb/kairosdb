package org.kairosdb.core.datastore;

/**
 Created by bhawkins on 11/22/14.

 When a query plugin is specified the instance is instantiated from Guice
 and properties specified in the query are injected into the object using
 bean setters.  If returning a singleton it must be thread safe.
 */
public interface QueryPlugin
{
	public String getName();
}
