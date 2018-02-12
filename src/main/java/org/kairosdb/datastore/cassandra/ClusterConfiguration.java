package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.ConsistencyLevel;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import org.kairosdb.core.KairosConfig;

import java.util.Collections;
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
	private String m_localDCName;
	private String m_replication;

	public ClusterConfiguration(KairosConfig config)
	{
		//todo load defaults into a config before getting values using withfallback

		m_keyspace = config.getString("keyspace", "kairosdb");
		m_replication = config.getString("replication", "{'class': 'SimpleStrategy','replication_factor' : 1}");
		m_clusterName = config.getString("name", "default");
		m_readConsistencyLevel = ConsistencyLevel.valueOf(config.getString("read_consistency_level", "ONE"));
		m_writeConsistencyLevel = ConsistencyLevel.valueOf(config.getString("write_consistency_level", "QUORUM"));

		m_useSsl = config.getBoolean("use_ssl", false);
		m_maxQueueSize = config.getInt("max_queue_size", 500);

		m_connectionsLocalCore = config.getInt("connections_per_host.local.core", 5);
		m_connectionsLocalMax = config.getInt("connections_per_host.local.max", 100);
		m_connectionsRemoteCore = config.getInt("connections_per_host.remote.core", 1);
		m_connectionsRemoteMax = config.getInt("connections_per_host.remote.max", 10);
		
		m_requestsPerConnectionLocal = config.getInt("max_requests_per_connection.local",128);
		m_requestsPerConnectionRemote = config.getInt("max_requests_per_connection.remote", 128);

		m_hostList = config.getStringList("cql_host_list", Collections.singletonList("localhost"));

		if (config.hasPath("local_dc_name"))
			m_localDCName = config.getString("local_dc_name");

		//System.out.println("Hosts: "+m_hostList);

		if (config.hasPath("auth.user_name"))
			m_authUser = config.getString("auth.user_name");

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

	public String getLocalDCName()
	{
		return m_localDCName;
	}

	public String getReplication()
	{
		return m_replication;
	}
}
