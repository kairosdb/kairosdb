package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.typesafe.config.Config;
import org.kairosdb.core.KairosConfig;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 Created by bhawkins on 10/13/14.
 */
public class CassandraConfiguration
{
	public static final String READ_CONSISTENCY_LEVEL = "kairosdb.datastore.cassandra.read_consistency_level";
	public static final String WRITE_CONSISTENCY_LEVEL = "kairosdb.datastore.cassandra.write_consistency_level";
	public static final String DATAPOINT_TTL = "kairosdb.datastore.cassandra.datapoint_ttl";

	public static final String ROW_KEY_CACHE_SIZE_PROPERTY = "kairosdb.datastore.cassandra.row_key_cache_size";
	public static final String STRING_CACHE_SIZE_PROPERTY = "kairosdb.datastore.cassandra.string_cache_size";

	public static final String KEYSPACE_PROPERTY = "kairosdb.datastore.cassandra.keyspace";
	public static final String HOST_LIST_PROPERTY = "kairosdb.datastore.cassandra.cql_host_list";
	public static final String SIMULTANEOUS_QUERIES = "kairosdb.datastore.cassandra.simultaneous_cql_queries";
	public static final String QUERY_LIMIT = "kairosdb.datastore.cassandra.query_limit";
	public static final String QUERY_READER_THREADS = "kairosdb.datastore.cassandra.query_reader_threads";

	public static final String AUTH_USER_NAME = "kairosdb.datastore.cassandra.auth.user_name";
	public static final String AUTH_PASSWORD = "kairosdb.datastore.cassandra.auth.password";
	public static final String USE_SSL = "kairosdb.datastore.cassandra.use_ssl";

	public static final String LOCAL_CORE_CONNECTIONS = "kairosdb.datastore.cassandra.connections_per_host.local.core";
	public static final String LOCAL_MAX_CONNECTIONS = "kairosdb.datastore.cassandra.connections_per_host.local.max";

	public static final String REMOTE_CORE_CONNECTIONS = "kairosdb.datastore.cassandra.connections_per_host.remote.core";
	public static final String REMOTE_MAX_CONNECTIONS = "kairosdb.datastore.cassandra.connections_per_host.remote.max";

	public static final String LOCAL_MAX_REQ_PER_CONN = "kairosdb.datastore.cassandra.max_requests_per_connection.local";
	public static final String REMOTE_MAX_REQ_PER_CONN = "kairosdb.datastore.cassandra.max_requests_per_connection.remote";

	public static final String MAX_QUEUE_SIZE = "kairosdb.datastore.cassandra.max_queue_size";
	
	public static final String LOCAL_DATACENTER = "kairosdb.datastore.cassandra.local_datacenter";


	@Inject(optional = true)
	@Named(DATAPOINT_TTL)
	private int m_datapointTtl = 0; //Zero ttl means data lives forever.

	@Inject
	@Named(ROW_KEY_CACHE_SIZE_PROPERTY)
	private int m_rowKeyCacheSize = 1024;

	@Inject
	@Named(STRING_CACHE_SIZE_PROPERTY)
	private int m_stringCacheSize = 1024;

	@Inject
	@Named(CassandraModule.CASSANDRA_AUTH_MAP)
	private Map<String, String> m_cassandraAuthentication;

	@Inject
	@Named(SIMULTANEOUS_QUERIES)
	private int m_simultaneousQueries = 20;

	@Inject
	@Named(QUERY_READER_THREADS)
	private int m_queryReaderThreads = 6;

	@Inject(optional = true)
	@Named(QUERY_LIMIT)
	private int m_queryLimit = 0;


	private final ClusterConfiguration m_writeCluster;
	private final ClusterConfiguration m_metaCluster;

	private final List<ClusterConfiguration> m_readClusters;

	@Named(MAX_QUEUE_SIZE)
	private int m_maxQueueSize = 500;
	
	@Inject(optional = true)
	@Named(LOCAL_DATACENTER)
	private String m_localDatacenter;

	@Inject
	public CassandraConfiguration(KairosConfig kairosConfig)
	{
		Config config = kairosConfig.getConfig();
		Config writeConfig = config.getConfig("kairosdb.datastore.cassandra.write_cluster");

		m_writeCluster = new ClusterConfiguration(writeConfig);

		if (config.hasPath("kairosdb.datastore.cassandra.meta_cluster"))
		{
			m_metaCluster = new ClusterConfiguration(config.getConfig("kairosdb.datastore.cassandra.meta_cluster"));
		}
		else
			m_metaCluster = m_writeCluster;

		if (config.hasPath("kairosdb.datastore.cassandra.read_clusters"))
		{
			System.out.println("LOADING READ CLUSTERS");
			List<? extends Config> clientList = config.getConfigList("kairosdb.datastore.cassandra.read_clusters");

			ImmutableList.Builder<ClusterConfiguration> readClusterBuilder = new ImmutableList.Builder<>();
			for (Config client : clientList)
			{
				readClusterBuilder.add(new ClusterConfiguration(client));
			}

			m_readClusters = readClusterBuilder.build();
		}
		else
			m_readClusters = ImmutableList.of();
	}


	public int getDatapointTtl()
	{
		return m_datapointTtl;
	}

	public int getRowKeyCacheSize()
	{
		return m_rowKeyCacheSize;
	}

	public int getStringCacheSize()
	{
		return m_stringCacheSize;
	}

	public int getSimultaneousQueries()
	{
		return m_simultaneousQueries;
	}

	public int getQueryReaderThreads()
	{
		return m_queryReaderThreads;
	}

	public int getQueryLimit()
	{
		return m_queryLimit;
	}
	
	public String getLocalDatacenter()
	{
		return m_localDatacenter;
	}

	public ClusterConfiguration getWriteCluster()
	{
		return m_writeCluster;
	}

	public ClusterConfiguration getMetaCluster()
	{
		return m_metaCluster;
	}

	public List<ClusterConfiguration> getReadClusters()
	{
		return m_readClusters;
	}
}
