package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import org.kairosdb.core.KairosConfig;

import java.text.ParseException;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

public class ClusterConfiguration
{
	private final Splitter PortSplitter = Splitter.on(':')
			.trimResults().omitEmptyStrings();

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
	private final int m_requestRetryCount;
	private final Map<String, Integer> m_hostList;
	private final String m_clusterName;
	private String m_authPassword;
	private String m_authUser;
	private String m_localDCName;
	private String m_replication;
	private long m_startTime;
	private long m_endTime;

	public ClusterConfiguration(KairosConfig config) throws ParseException
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

		m_requestsPerConnectionLocal = config.getInt("max_requests_per_connection.local", 128);
		m_requestsPerConnectionRemote = config.getInt("max_requests_per_connection.remote", 128);

		m_requestRetryCount = config.getInt("request_retry_count", 2);

		List<String> hostList = config.getStringList("cql_host_list", Collections.singletonList("localhost"));

		ImmutableMap.Builder<String, Integer> hostBuilder = ImmutableMap.<String, Integer>builder();
		for (String hostEntry : hostList)
		{
			Iterator<String> hostPort = PortSplitter.split(hostEntry).iterator();

			String host = hostPort.next();
			int port = 9042;

			if (hostPort.hasNext())
				port = Integer.parseInt(hostPort.next());

			hostBuilder.put(host, port);
		}

		m_hostList = hostBuilder.build();

		if (config.hasPath("local_dc_name"))
			m_localDCName = config.getString("local_dc_name");

		//System.out.println("Hosts: "+m_hostList);

		if (config.hasPath("auth.user_name"))
			m_authUser = config.getString("auth.user_name");

		if (config.hasPath("auth.password"))
			m_authPassword = config.getString("auth.password");

		Date startDate = config.getDateTime("start_time");
		if (startDate != null)
			m_startTime = startDate.getTime();
		else
			m_startTime = Long.MIN_VALUE;

		Date endDate = config.getDateTime("end_time");
		if (endDate != null)
			m_endTime = endDate.getTime();
		else
			m_endTime = Long.MAX_VALUE;

		checkState(m_startTime < m_endTime, "Cluster start time must be before end time");
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

	public Map<String, Integer> getHostList()
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

	public int getRequestRetryCount()
	{
		return m_requestRetryCount;
	}

	public long getStartTime()
	{
		return m_startTime;
	}

	public long getEndTime()
	{
		return m_endTime;
	}

	public boolean containRange(long queryStartTime, long queryEndTime)
	{
		return (!(queryEndTime < m_startTime || queryStartTime > m_endTime));
	}
}

