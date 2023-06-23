package org.kairosdb.datastore.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;

/**
 Created by bhawkins on 2/9/16.
 */
public interface CassandraClient
{
	CqlSession getKeyspaceSession();

	CqlSession getSession();

	String getKeyspace();

	String getReplication();

	LoadBalancingPolicy getWriteLoadBalancingPolicy();

	ClusterConfiguration getClusterConfiguration();

	void close();

	void init();
}
