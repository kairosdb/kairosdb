package org.kairosdb.datastore.cassandra;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import com.datastax.driver.core.ConsistencyLevel;

import java.util.Map;

/**
 Created by bhawkins on 10/13/14.
 */
public class CassandraConfiguration
{
	private static final String HOST_LIST_PROPERTY = "kairosdb.datastore.cassandra.host_list";

	public static final String READ_CONSISTENCY_LEVEL = "kairosdb.datastore.cassandra.read_consistency_level";
	public static final String WRITE_CONSISTENCY_LEVEL_META = "kairosdb.datastore.cassandra.write_consistency_level_meta";
	public static final String WRITE_CONSISTENCY_LEVEL_DATAPOINT = "kairosdb.datastore.cassandra.write_consistency_level_datapoint";

	public static final String DATAPOINT_TTL = "kairosdb.datastore.cassandra.datapoint_ttl";

	public static final String ROW_KEY_CACHE_SIZE_PROPERTY = "kairosdb.datastore.cassandra.row_key_cache_size";
	public static final String STRING_CACHE_SIZE_PROPERTY = "kairosdb.datastore.cassandra.string_cache_size";

	public static final String KEYSPACE_PROPERTY = "kairosdb.datastore.cassandra.keyspace";
	public static final String REPLICATION_FACTOR_PROPERTY = "kairosdb.datastore.cassandra.replication_factor";

	@Inject
	@Named(WRITE_CONSISTENCY_LEVEL_META)
	private ConsistencyLevel m_dataWriteLevelMeta = ConsistencyLevel.LOCAL_ONE;

	@Inject
	@Named(WRITE_CONSISTENCY_LEVEL_DATAPOINT)
	private ConsistencyLevel m_dataWriteLevelDataPoint = ConsistencyLevel.LOCAL_ONE;

	@Inject
	@Named(READ_CONSISTENCY_LEVEL)
	private ConsistencyLevel m_dataReadLevel = ConsistencyLevel.LOCAL_ONE;

	@Inject(optional=true)
	@Named(DATAPOINT_TTL)
	private int m_datapointTtl = 0; //Zero ttl means data lives forever.

	@Inject
	@Named(ROW_KEY_CACHE_SIZE_PROPERTY)
	private int m_rowKeyCacheSize = 1024;

	@Inject
	@Named(HOST_LIST_PROPERTY)
	private String m_hostList = "localhost:9042";

	@Inject
	@Named(STRING_CACHE_SIZE_PROPERTY)
	private int m_stringCacheSize = 1024;

	@Inject
	@Named(CassandraModule.CASSANDRA_AUTH_MAP)
	private Map<String, String> m_cassandraAuthentication;

	@Inject
	@Named(REPLICATION_FACTOR_PROPERTY)
	private int m_replicationFactor;

	@Inject
	@Named(KEYSPACE_PROPERTY)
	private String m_keyspaceName;


	public CassandraConfiguration()
	{
	}

	public CassandraConfiguration(int replicationFactor,
			int singleRowReadSize,
			int multiRowSize,
			int multiRowReadSize,
			int writeDelay,
			int maxWriteSize,
			String keyspaceName)
	{
		m_replicationFactor = replicationFactor;
		m_keyspaceName = keyspaceName;
	}

	public ConsistencyLevel getDataWriteLevelMeta()
	{
		return m_dataWriteLevelMeta;
	}

	public ConsistencyLevel getDataWriteLevelDataPoint()
	{
		return m_dataWriteLevelDataPoint;
	}

	public ConsistencyLevel getDataReadLevel()
	{
		return m_dataReadLevel;
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

	public Map<String, String> getCassandraAuthentication()
	{
		return m_cassandraAuthentication;
	}

	public int getReplicationFactor()
	{
		return m_replicationFactor;
	}

	public String getKeyspaceName()
	{
		return m_keyspaceName;
	}

	public String getHostList() {
		return m_hostList;
	}

	public void setHostList(String m_hostList) {
		this.m_hostList = m_hostList;
	}
}
