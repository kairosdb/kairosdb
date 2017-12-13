package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.ConsistencyLevel;
import com.typesafe.config.Config;

public class ClusterConfiguration
{

	private final String m_keyspace;
	private final ConsistencyLevel m_readConsistencyLevel;
	private final ConsistencyLevel m_writeConsistencyLevel;
	private final boolean m_useSsl;
	private final int m_maxQueueSize;
	private final int m_connectionsLocalCore;
	private final int m_connectionsLocalMax;
	private final int m_connectionsRemoteCore;
	private final int m_connectionsRemoteMax;
	private final int m_requestsPerConnectionLocal;
	private final int m_requestsPerConnectionRemote;

	public ClusterConfiguration(Config config)
	{
		//todo load defaults into a config before getting values using withfallback

		m_keyspace = config.getString("keyspace");
		m_readConsistencyLevel = ConsistencyLevel.valueOf(config.getString("read_consistency_level"));
		m_writeConsistencyLevel = ConsistencyLevel.valueOf(config.getString("write_consistency_level");

		m_useSsl = config.getBoolean("use_ssl");
		m_maxQueueSize = config.getInt("max_queue_size");

		m_connectionsLocalCore = config.getInt("connections_per_host.local.core");
		m_connectionsLocalMax = config.getInt("connections_per_host.local.max");
		m_connectionsRemoteCore = config.getInt("connections_per_host.remote.core");
		m_connectionsRemoteMax = config.getInt("connections_per_host.remote.max");
		
		m_requestsPerConnectionLocal = config.getInt("max_requests_per_connection.local");
		m_requestsPerConnectionRemote = config.getInt("max_requests_per_connection.remote");
	}
}
