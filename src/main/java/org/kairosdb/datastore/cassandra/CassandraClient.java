package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.Session;

/**
 Created by bhawkins on 2/9/16.
 */
public interface CassandraClient
{
	Session getKeyspaceSession();

	Session getSession();

	String getKeyspace();

	void close();
}
