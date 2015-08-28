package org.kairosdb.core.datastore;

/**
 Created by bhawkins on 11/23/14.
 */
public interface QueryPluginFactory
{
	public QueryPlugin createQueryPlugin(String name);
}
