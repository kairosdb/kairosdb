package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.util.Map;


/**
 Created by bhawkins on 10/13/14.
 */
public class CassandraConfiguration {

	private static final String READ_CONSISTENCY_LEVEL = "kairosdb.datastore.cassandra.read_consistency_level";
	private static final String WRITE_CONSISTENCY_LEVEL_META = "kairosdb.datastore.cassandra.write_consistency_level_meta";
	private static final String WRITE_CONSISTENCY_LEVEL_DATAPOINT = "kairosdb.datastore.cassandra.write_consistency_level_datapoint";
	private static final String DATAPOINT_TTL = "kairosdb.datastore.cassandra.datapoint_ttl";
	private static final String ROW_KEY_CACHE_SIZE_PROPERTY = "kairosdb.datastore.cassandra.row_key_cache_size";
	private static final String STRING_CACHE_SIZE_PROPERTY = "kairosdb.datastore.cassandra.string_cache_size";
	private static final String TAG_NAME_CACHE_SIZE_PROPERTY = "kairosdb.datastore.cassandra.tag_name_cache_size";
	private static final String TAG_VALUE_CACHE_SIZE_PROPERTY = "kairosdb.datastore.cassandra.tag_value_cache_size";
	private static final String METRIC_NAME_CACHE_SIZE_PROPERTY = "kairosdb.datastore.cassandra.metric_name_cache_size";
	private static final String KEYSPACE_PROPERTY = "kairosdb.datastore.cassandra.keyspace";
	private static final String REPLICATION_FACTOR_PROPERTY = "kairosdb.datastore.cassandra.replication_factor";
	private static final String CASSANDRA_USER = "kairosdb.datastore.cassandra.user";
	private static final String CASSANDRA_PASSWORD = "kairosdb.datastore.cassandra.password";
	private static final String CASSANDRA_ADDRESS_TRANSLATOR = "kairosdb.datastore.cassandra.address_translator";
	private static final String CASSANDRA_READ_ROWWIDTH = "kairosdb.datastore.cassandra.read_row_width";
	private static final String CASSANDRA_WRITE_ROWWIDTH = "kairosdb.datastore.cassandra.write_row_width";
	private static final String CASSANDRA_ROW_SHIFT = "kairosdb.datastore.cassandra.row_shift";
	private static final String CASSANDRA_MAX_ROW_KEYS_FOR_QUERY = "kairosdb.datastore.cassandra.max_row_keys_for_query";
	private static final String CASSANDRA_MAX_ROWS_FOR_KEY_QUERY = "kairosdb.datastore.cassandra.max_rows_for_key_query";
	private static final String CASSANDRA_INDEX_TAG_LIST = "kairosdb.datastore.cassandra.index_tag_list";
	private static final String CASSANDRA_METRIC_INDEX_TAG_LIST = "kairosdb.datastore.cassandra.metric_index_tag_list";
	private static final String NEW_SPLIT_INDEX_START_TIME_MS = "kairosdb.datastore.cassandra.new_split_index_start_time_ms";
	private static final String USE_TIME_INDEX_READ = "kairosdb.datastore.cassandra.use_time_index_read";
	private static final String USE_TIME_INDEX_WRITE = "kairosdb.datastore.cassandra.use_time_index_write";
	private static final String HOST_LIST_PROPERTY = "kairosdb.datastore.cassandra.host_list";
	private static final String CASSANDRA_PORT = "kairosdb.datastore.cassandra.port";
	private static final String QUERY_SAMPLING_PERCENTAGE = "kairosdb.datastore.cassandra.query_sampling_percentage";
	@Inject(optional = true)
	@Named(USE_TIME_INDEX_READ)
	private boolean m_useTimeIndexRead = false;
	@Inject(optional = true)
	@Named(USE_TIME_INDEX_WRITE)
	private boolean m_useTimeIndexWrite = false;
	@Inject(optional = true)
	@Named(NEW_SPLIT_INDEX_START_TIME_MS)
	private long m_newSplitIndexStartTimeMs = 0L;
	@Inject(optional=true)
	@Named(CASSANDRA_INDEX_TAG_LIST)
	private String m_indexTagList = "key,application_id,stack_name";
	/**
	 * Format: zmon.check.1=entity,key,application_id,stack_name;zmon.check.2=stack_name,application_id,key
	 */
	@Inject(optional=true)
	@Named(CASSANDRA_METRIC_INDEX_TAG_LIST)
	private String m_metricIndexTagList = "";
	@Inject(optional=true)
	@Named(CASSANDRA_MAX_ROW_KEYS_FOR_QUERY)
	private int m_maxRowKeysForQuery = 10000;
	@Inject(optional=true)
	@Named(CASSANDRA_MAX_ROWS_FOR_KEY_QUERY)
	private int m_maxRowsForKeysQuery = 300000;
	@Inject(optional=true)
	@Named(CASSANDRA_WRITE_ROWWIDTH)
	private long m_rowWidthWrite = 1L * 3 * 24 * 60 * 60 * 1000; // 3 day row width for write
	@Inject(optional=true)
	@Named(CASSANDRA_READ_ROWWIDTH)
	private long m_rowWidthRead = 1814400000L; // 3 weeks for reading - backwards compatible
	@Inject(optional=true)
	@Named(CASSANDRA_ROW_SHIFT)
	private long m_rowShift = 0;
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
	@Named(HOST_LIST_PROPERTY)
	private String m_hostList = "localhost";
	@Inject(optional=true)
	@Named(STRING_CACHE_SIZE_PROPERTY)
	private int m_stringCacheSize = 1024;
	@Inject(optional=true)
	@Named(ROW_KEY_CACHE_SIZE_PROPERTY)
	private int m_rowKeyCacheSize = 1024;
	@Inject(optional=true)
	@Named(TAG_NAME_CACHE_SIZE_PROPERTY)
	private int m_tagNameCacheSize = 1024;
	@Inject(optional=true)
	@Named(TAG_VALUE_CACHE_SIZE_PROPERTY)
	private int m_tagValueCacheSize = 1024;
	@Inject(optional=true)
	@Named(METRIC_NAME_CACHE_SIZE_PROPERTY)
	private int m_metricNameCacheSize = 1024;
	@Inject
	@Named(CassandraModule.CASSANDRA_AUTH_MAP)
	private Map<String, String> m_cassandraAuthentication;
	@Inject
	@Named(REPLICATION_FACTOR_PROPERTY)
	private int m_replicationFactor;
	@Inject(optional=true)
	@Named(KEYSPACE_PROPERTY)
	private String m_keyspaceName = "kairosdb";
	@Inject(optional=true)
	@Named(CASSANDRA_USER)
	private String m_user = null;
	@Inject(optional=true)
	@Named(CASSANDRA_PASSWORD)
	private String m_password = null;
	@Inject(optional=true)
	@Named(CASSANDRA_ADDRESS_TRANSLATOR)
	private ADDRESS_TRANSLATOR_TYPE m_addressTranslator = ADDRESS_TRANSLATOR_TYPE.NONE;
	@Inject(optional=true)
	@Named(CASSANDRA_PORT)
	private int m_port = 9042;
	@Inject(optional=true)
	@Named(QUERY_SAMPLING_PERCENTAGE)
	private int querySamplingPercentage = 20;

	public CassandraConfiguration()
	{
	}

	public CassandraConfiguration(int replicationFactor,
			int singleRowReadSize,
			int multiRowSize,
			int multiRowReadSize,
			int writeDelay,
			int maxWriteSize,
			String hostList,
			String keyspaceName)
	{
		m_replicationFactor = replicationFactor;
		m_hostList = hostList;
		m_keyspaceName = keyspaceName;
	}

	public boolean isUseTimeIndexRead() {
		return m_useTimeIndexRead;
	}

	public boolean isUseTimeIndexWrite() {
		return m_useTimeIndexWrite;
	}

	public long getNewSplitIndexStartTimeMs() {
		return m_newSplitIndexStartTimeMs;
	}

	public String getIndexTagList() {
		return m_indexTagList;
	}

	public String getMetricIndexTagList() {
		return m_metricIndexTagList;
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

	public String getPassword() {
		return m_password;
	}

	public String getUser() {
		return m_user;
	}

	public ADDRESS_TRANSLATOR_TYPE getAddressTranslator() {
		return m_addressTranslator;
	}

	public int getPort() {
		return m_port;
	}

	public long getRowWidthRead() {
		return m_rowWidthRead;
	}

	public long getRowWidthWrite() {
		return m_rowWidthWrite;
	}

	public int getMaxRowKeysForQuery() {
		return m_maxRowKeysForQuery;
	}

	public int getMaxRowsForKeysQuery() {
		return m_maxRowsForKeysQuery;
	}

	public int getTagNameCacheSize() {
		return m_tagNameCacheSize;
	}

	public int getTagValueCacheSize() {
		return m_tagValueCacheSize;
	}

	public int getMetricNameCacheSize() {
		return m_metricNameCacheSize;
	}

	public int getQuerySamplingPercentage() {
		return querySamplingPercentage;
	}

	public void setQuerySamplingPercentage(int querySamplingPercentage) {
		this.querySamplingPercentage = querySamplingPercentage;
	}

	public long getRowShift() {
		return m_rowShift;
	}

	public static enum ADDRESS_TRANSLATOR_TYPE {
		NONE,
		EC2
	}

}
