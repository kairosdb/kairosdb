package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.ConsistencyLevel;
import com.typesafe.config.Config;

import java.util.List;

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
	private final List<String> m_hostList;
	private final String m_clusterName;
	private String m_authPassword;
	private String m_authUser;

	public ClusterConfiguration(Config config)
	{
		//todo load defaults into a config before getting values using withfallback

		m_keyspace = config.getString("keyspace");
		m_clusterName = config.getString("name");
		m_readConsistencyLevel = ConsistencyLevel.valueOf(config.getString("read_consistency_level"));
		m_writeConsistencyLevel = ConsistencyLevel.valueOf(config.getString("write_consistency_level"));

		m_useSsl = config.getBoolean("use_ssl");
		m_maxQueueSize = config.getInt("max_queue_size");

		m_connectionsLocalCore = config.getInt("connections_per_host.local.core");
		m_connectionsLocalMax = config.getInt("connections_per_host.local.max");
		m_connectionsRemoteCore = config.getInt("connections_per_host.remote.core");
		m_connectionsRemoteMax = config.getInt("connections_per_host.remote.max");
		
		m_requestsPerConnectionLocal = config.getInt("max_requests_per_connection.local");
		m_requestsPerConnectionRemote = config.getInt("max_requests_per_connection.remote");

		m_hostList = config.getStringList("cql_host_list");

		System.out.println("Hosts: "+m_hostList);

		if (config.hasPath("auth.username"))
			m_authUser = config.getString("auth.username");

		if (config.hasPath("auth.password"))
			m_authPassword = config.getString("auth.password");
	}

	public String getKeyspace()
	{
		return m_keyspace;
	}

	public ConsistencyLevel getReadConsistencyLevel()
	{
		return m_readConsistencyLevel;
	}

	public ConsistencyLevel getWriteConsistencyLevel()
	{
		return m_writeConsistencyLevel;
	}

	public boolean isUseSsl()
	{
		return m_useSsl;
	}

	public int getMaxQueueSize()
	{
		return m_maxQueueSize;
	}

	public int getConnectionsLocalCore()
	{
		return m_connectionsLocalCore;
	}

	public int getConnectionsLocalMax()
	{
		return m_connectionsLocalMax;
	}

	public int getConnectionsRemoteCore()
	{
		return m_connectionsRemoteCore;
	}

	public int getConnectionsRemoteMax()
	{
		return m_connectionsRemoteMax;
	}

	public int getRequestsPerConnectionLocal()
	{
		return m_requestsPerConnectionLocal;
	}

	public int getRequestsPerConnectionRemote()
	{
		return m_requestsPerConnectionRemote;
	}

	public List<String> getHostList()
	{
		return m_hostList;
	}

	public String getAuthPassword()
	{
		return m_authPassword;
	}

	public String getAuthUser()
	{
		return m_authUser;
	}

	public String getClusterName()
	{
		return m_clusterName;
	}
}
