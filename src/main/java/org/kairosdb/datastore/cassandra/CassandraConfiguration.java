package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.inject.Inject;
import com.google.inject.name.Named;

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
	public static final String SIMULTANIOUS_QUERIES = "kairosdb.datastore.cassandra.simultaneous_cql_queries";


	@Inject
	@Named(WRITE_CONSISTENCY_LEVEL)
	private ConsistencyLevel m_dataWriteLevel = ConsistencyLevel.QUORUM;

	@Inject
	@Named(READ_CONSISTENCY_LEVEL)
	private ConsistencyLevel m_dataReadLevel = ConsistencyLevel.ONE;

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
	@Named(SIMULTANIOUS_QUERIES)
	private int m_simultaneousQueries = 100;

	@Inject
	@Named(KEYSPACE_PROPERTY)
	private String m_keyspaceName;


	public CassandraConfiguration()
	{
	}

	public CassandraConfiguration(String keyspaceName)
	{
		m_keyspaceName = keyspaceName;
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
}
