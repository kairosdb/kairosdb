package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.base.Splitter;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 Created by bhawkins on 10/13/14.
 */
public class CassandraConfiguration
{
	public static final String READ_CONSISTENCY_LEVEL = "kairosdb.datastore.cassandra.read_consistency_level";
	public static final String WRITE_CONSISTENCY_LEVEL = "kairosdb.datastore.cassandra.write_consistency_level";
	public static final String DATAPOINT_TTL = "kairosdb.datastore.cassandra.datapoint_ttl";
	
	public static final String ALIGN_DATAPOINT_TTL_WITH_TIMESTAMP = "kairosdb.datastore.cassandra.align_datapoint_ttl_with_timestamp";
	public static final String FORCE_DEFAULT_DATAPOINT_TTL = "kairosdb.datastore.cassandra.force_default_datapoint_ttl";

	public static final String ROW_KEY_CACHE_SIZE_PROPERTY = "kairosdb.datastore.cassandra.row_key_cache_size";
	public static final String STRING_CACHE_SIZE_PROPERTY = "kairosdb.datastore.cassandra.string_cache_size";

	public static final String KEYSPACE_PROPERTY = "kairosdb.datastore.cassandra.keyspace";
	public static final String REPLICATION_PROPERTY = "kairosdb.datastore.cassandra.replication";
	public static final String HOST_LIST_PROPERTY = "kairosdb.datastore.cassandra.cql_host_list";
	public static final String SIMULTANIOUS_QUERIES = "kairosdb.datastore.cassandra.simultaneous_cql_queries";
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

	public static final String CREATE_SCHEMA_PROPERTY = "kairosdb.datastore.cassandra.create_schema";

	public static final String CONNECTION_TIMEOUT_PROPERTY = "kairosdb.datastore.cassandra.connection_timeout";
	public static final String READ_TIMEOUT_PROPERTY = "kairosdb.datastore.cassandra.read_timeout";


	@Inject
	@Named(WRITE_CONSISTENCY_LEVEL)
	private ConsistencyLevel m_dataWriteLevel = ConsistencyLevel.QUORUM;

	@Inject
	@Named(READ_CONSISTENCY_LEVEL)
	private ConsistencyLevel m_dataReadLevel = ConsistencyLevel.ONE;

	@Inject(optional = true)
	@Named(DATAPOINT_TTL)
	private int m_datapointTtl = 0; //Zero ttl means data lives forever.

	@Inject(optional = true)
	@Named(ALIGN_DATAPOINT_TTL_WITH_TIMESTAMP)
	private boolean m_alignDatapointTtlWithTimestamp = false;
	
	@Inject(optional = true)
	@Named(FORCE_DEFAULT_DATAPOINT_TTL)
	private boolean m_forceDefaultDatapointTtl = false;

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
	@Named(SIMULTANIOUS_QUERIES)
	private int m_simultaneousQueries = 20;

	@Inject
	@Named(QUERY_READER_THREADS)
	private int m_queryReaderThreads = 6;

	@Inject(optional = true)
	@Named(QUERY_LIMIT)
	private int m_queryLimit = 0;

	@Inject
	@Named(KEYSPACE_PROPERTY)
	private String m_keyspaceName;

	@Inject
	@Named(REPLICATION_PROPERTY)
	private String m_replication = "{'class': 'SimpleStrategy','replication_factor' : 1}";

	private Map<String, Integer> m_hostList = new HashMap<>();

	@Inject(optional = true)
	@Named(AUTH_USER_NAME)
	private String m_authUserName;

	@Inject(optional = true)
	@Named(AUTH_PASSWORD)
	private String m_authPassword;

	@Inject
	@Named(USE_SSL)
	private boolean m_useSsl;

	@Inject
	@Named(LOCAL_CORE_CONNECTIONS)
	private int m_localCoreConnections = 5;

	@Inject
	@Named(LOCAL_MAX_CONNECTIONS)
	private int m_localMaxConnections = 100;

	@Inject
	@Named(REMOTE_CORE_CONNECTIONS)
	private int m_remoteCoreConnections = 1;

	@Inject
	@Named(REMOTE_MAX_CONNECTIONS)
	private int m_remoteMaxConnections = 10;

	@Inject
	@Named(LOCAL_MAX_REQ_PER_CONN)
	private int m_localMaxReqPerConn = 128;

	@Inject
	@Named(REMOTE_MAX_REQ_PER_CONN)
	private int m_remoteMaxReqPerConn = 128;

	@Inject
	@Named(MAX_QUEUE_SIZE)
	private int m_maxQueueSize = 500;
	
	@Inject(optional = true)
	@Named(LOCAL_DATACENTER)
	private String m_localDatacenter;

	@Inject
	@Named(CREATE_SCHEMA_PROPERTY)
	private boolean m_createSchema;

	@Inject
	@Named(CONNECTION_TIMEOUT_PROPERTY)
	private int m_connectionTimeout = 5000;

	@Inject
	@Named(READ_TIMEOUT_PROPERTY)
	private int m_readTimeout = 12000;

	public CassandraConfiguration()
	{
	}

	public CassandraConfiguration(String keyspaceName)
	{
		m_keyspaceName = keyspaceName;
	}

	public Map<String, Integer> getHostList()
	{
		return m_hostList;
	}

	private final Splitter HostSplitter = Splitter.on(',')
			.trimResults().omitEmptyStrings();

	private final Splitter PortSplitter = Splitter.on(':')
			.trimResults().omitEmptyStrings();

	@Inject
	public void setHostList(@Named(HOST_LIST_PROPERTY) String hostList)
	{
		Iterable<String> strHostList = HostSplitter.split(hostList);
		for (String hostEntry : strHostList)
		{
			Iterator<String> hostPort = PortSplitter.split(hostEntry).iterator();

			String host = hostPort.next();
			int port = 9042;

			if (hostPort.hasNext())
				port = Integer.parseInt(hostPort.next());

			m_hostList.put(host, port);
		}

	}

	public ConsistencyLevel getDataWriteLevel()
	{
		return m_dataWriteLevel;
	}

	public ConsistencyLevel getDataReadLevel()
	{
		return m_dataReadLevel;
	}

	public int getDatapointTtl()
	{
		return m_datapointTtl;
	}
	
	public boolean isAlignDatapointTtlWithTimestamp() 
	{
		return m_alignDatapointTtlWithTimestamp;
	}
	
	public boolean isForceDefaultDatapointTtl()
	{
		return m_forceDefaultDatapointTtl;
	}

	public int getRowKeyCacheSize()
	{
		return m_rowKeyCacheSize;
	}

	public int getStringCacheSize()
	{
		return m_stringCacheSize;
	}

	public Map<String, String> getCassandraAuthentication()
	{
		return m_cassandraAuthentication;
	}

	public String getKeyspaceName()
	{
		return m_keyspaceName;
	}

	public int getSimultaneousQueries()
	{
		return m_simultaneousQueries;
	}

	public String getAuthUserName()
	{
		return m_authUserName;
	}

	public String getAuthPassword()
	{
		return m_authPassword;
	}

	public int getLocalCoreConnections()
	{
		return m_localCoreConnections;
	}

	public int getLocalMaxConnections()
	{
		return m_localMaxConnections;
	}

	public int getRemoteCoreConnections()
	{
		return m_remoteCoreConnections;
	}

	public int getRemoteMaxConnections()
	{
		return m_remoteMaxConnections;
	}

	public int getLocalMaxReqPerConn()
	{
		return m_localMaxReqPerConn;
	}

	public int getRemoteMaxReqPerConn()
	{
		return m_remoteMaxReqPerConn;
	}

	public int getMaxQueueSize()
	{
		return m_maxQueueSize;
	}
	
	public String getLocalDatacenter()
	{
		return m_localDatacenter;
	}

	public int getQueryReaderThreads()
	{
		return m_queryReaderThreads;
	}

	public int getQueryLimit()
	{
		return m_queryLimit;
	}

	public boolean isUseSsl()
	{
		return m_useSsl;
	}

	public String getReplication()
	{
		return m_replication;
	}

	public boolean isCreateSchema()
	{
		return m_createSchema;
	}

	public int getConnectionTimeout()
	{
		return m_connectionTimeout;
	}

	public int getReadTimeout()
	{
		return m_readTimeout;
	}
}
