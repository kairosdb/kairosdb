package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.EC2AwareRoundRobinPolicy;
import com.datastax.driver.core.policies.EC2MultiRegionAddressTranslator;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.google.inject.Inject;
/**
 Created by bhawkins on 3/4/15.
 */
public class CassandraClientImpl implements CassandraClient
{
	private final Cluster m_cluster;
	private String m_keyspace;

	@Inject
	public CassandraClientImpl(CassandraConfiguration config)
	{
		final Cluster.Builder builder = new Cluster.Builder();
		if(config.getAddressTranslator().equals(CassandraConfiguration.ADDRESS_TRANSLATOR_TYPE.EC2)) {
			builder.withAddressTranslator(new EC2MultiRegionAddressTranslator());
			// This should work, seems the EC2AwareRoundRobinPolicy uses REMOTE for not being in the SAME az
			builder.withLoadBalancingPolicy(new TokenAwarePolicy(EC2AwareRoundRobinPolicy.CreateEC2AwareRoundRobinPolicy()));
		}
		else {
			builder.withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()));
		}

		builder.withQueryOptions(new QueryOptions().setConsistencyLevel(config.getDataReadLevel()));

		for (String node : config.getHostList().split(",")) {
			builder.addContactPoint(node.trim());
		}

		String user = config.getUser();
		String password = config.getPassword();
		if(null!=user && null!=password && !"".equals(user) && !"".equals(password)) {
			builder.withCredentials(user, password);
		}

		m_cluster = builder.build();
		m_keyspace = config.getKeyspaceName();
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
