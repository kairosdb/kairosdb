package org.kairosdb.core.datastore;

import java.util.List;

/**
 Created by bhawkins on 5/18/17.
 */
public interface PluggableQuery
{
	List<QueryPlugin> getPlugins();
	void addPlugin(QueryPlugin plugin);
}
