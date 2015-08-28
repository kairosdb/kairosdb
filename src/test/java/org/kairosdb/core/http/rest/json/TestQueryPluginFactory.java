package org.kairosdb.core.http.rest.json;

import org.kairosdb.core.datastore.QueryPlugin;
import org.kairosdb.core.datastore.QueryPluginFactory;

/**
 Created by bhawkins on 3/27/15.
 */
public class TestQueryPluginFactory implements QueryPluginFactory
{
	@Override
	public QueryPlugin createQueryPlugin(String name)
	{
		return null;
	}
}
