package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

import java.util.List;

/**
 Created by bhawkins on 3/4/15.
 */
public class CassandraClientImpl implements CassandraClient
{
	private final Cluster m_cluster;
	private String m_keyspace;

	public CassandraClientImpl(String keyspace, List<String> nodes)
	{
		final Cluster.Builder builder = new Cluster.Builder()
				.withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))
				.withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.QUORUM));

		for (String node : nodes)
		{
			builder.addContactPoint(node);
		}

		m_cluster = builder.build();
		m_keyspace = keyspace;
	}


	@Override
	public Session getKeyspaceSession()
	{
		return m_cluster.connect(m_keyspace);
	}

	@Override
	public Session getSession()
	{
		return m_cluster.connect();
	}

	@Override
	public String getKeyspace()
	{
		return m_keyspace;
	}

	@Override
	public void close()
	{
		m_cluster.close();
	}


}
