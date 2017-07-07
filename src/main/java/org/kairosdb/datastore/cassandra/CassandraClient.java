package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.LoadBalancingPolicy;

/**
 Created by bhawkins on 2/9/16.
 */
public interface CassandraClient
{
	Session getKeyspaceSession();

	Session getSession();

	String getKeyspace();

	LoadBalancingPolicy getLoadBalancingPolicy();

	void close();
}
