/*
 * Copyright 2013 Proofpoint Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.SetMultimap;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.SliceQuery;
import org.h2.command.Prepared;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.datapoints.LegacyDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;
import org.kairosdb.core.datastore.*;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.reporting.ThreadReporter;
import org.kairosdb.util.KDataOutput;
import org.kairosdb.util.MemoryMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkNotNull;

public class CassandraDatastore implements Datastore
{
	public static final Logger logger = LoggerFactory.getLogger(CassandraDatastore.class);


	public static final String CREATE_KEYSPACE = "" +
			"CREATE KEYSPACE IF NOT EXISTS %s" +
			"  WITH REPLICATION = {'class': 'SimpleStrategy'," +
			"  'replication_factor' : 1}";

	public static final String DATA_POINTS_TABLE = "" +
			"CREATE TABLE IF NOT EXISTS data_points (\n" +
			"  key blob,\n" +
			"  column1 blob,\n" +
			"  value blob,\n" +
			"  PRIMARY KEY ((key), column1)\n" +
			") WITH COMPACT STORAGE";

	public static final String ROW_KEY_INDEX_TABLE = "" +
			"CREATE TABLE IF NOT EXISTS row_key_index (\n" +
			"  key blob,\n" +
			"  column1 blob,\n" +
			"  value blob,\n" +
			"  PRIMARY KEY ((key), column1)\n" +
			") WITH COMPACT STORAGE";

	public static final String STRING_INDEX_TABLE = "" +
			"CREATE TABLE IF NOT EXISTS string_index (\n" +
			"  key blob,\n" +
			"  column1 text,\n" +
			"  value blob,\n" +
			"  PRIMARY KEY ((key), column1)\n" +
			") WITH COMPACT STORAGE";


	public static final String DATA_POINTS_INSERT = "INSERT INTO data_points " +
			"(key, column1, value) VALUES (?, ?, ?) USING TTL ?";

	public static final String ROW_KEY_INDEX_INSERT = "INSERT INTO row_key_index " +
			"(key, column1, value) VALUES (?, ?, 0x00) USING TTL ?";

	public static final String STRING_INDEX_INSERT = "INSERT INTO string_index " +
			"(key, column1, value) VALUES (?, ?, 0x00)";

	public static final int LONG_FLAG = 0x0;
	public static final int FLOAT_FLAG = 0x1;

	public static final DataPointsRowKeySerializer DATA_POINTS_ROW_KEY_SERIALIZER = new DataPointsRowKeySerializer();


	public static final long ROW_WIDTH = 1814400000L; //3 Weeks wide

	public static final String KEY_QUERY_TIME = "kairosdb.datastore.cassandra.key_query_time";



	public static final String CF_DATA_POINTS = "data_points";
	public static final String CF_ROW_KEY_INDEX = "row_key_index";
	public static final String CF_STRING_INDEX = "string_index";

	public static final String ROW_KEY_METRIC_NAMES = "metric_names";
	public static final String ROW_KEY_TAG_NAMES = "tag_names";
	public static final String ROW_KEY_TAG_VALUES = "tag_values";
	private static final Charset UTF_8 = Charset.forName("UTF-8");

	private final Cluster m_cluster;
	private final Keyspace m_keyspace;



	//new properties
	CassandraClient m_cassandraClient;

	private Session m_session;
	private final PreparedStatement m_psInsertData;
	private final PreparedStatement m_psInsertRowKey;
	private final PreparedStatement m_psInsertString;
	private BatchStatement m_batchStatement;
	private final Object m_batchLock = new Object();
	//End new props


	private String m_keyspaceName;
	private int m_singleRowReadSize;
	private int m_multiRowSize;
	private int m_multiRowReadSize;
	//private WriteBuffer<DataPointsRowKey, Integer, byte[]> m_dataPointWriteBuffer;
	//private WriteBuffer<String, DataPointsRowKey, String> m_rowKeyWriteBuffer;
	//private WriteBuffer<String, String, String> m_stringIndexWriteBuffer;

	private DataCache<DataPointsRowKey> m_rowKeyCache = new DataCache<DataPointsRowKey>(1024);
	private DataCache<String> m_metricNameCache = new DataCache<String>(1024);
	private DataCache<String> m_tagNameCache = new DataCache<String>(1024);
	private DataCache<String> m_tagValueCache = new DataCache<String>(1024);

	private final KairosDataPointFactory m_kairosDataPointFactory;

	private CassandraConfiguration m_cassandraConfiguration;

	@Inject
	private LongDataPointFactory m_longDataPointFactory = new LongDataPointFactoryImpl();

	@Inject
	private List<RowKeyListener> m_rowKeyListeners = Collections.EMPTY_LIST;



	@Inject
	public CassandraDatastore(@Named("HOSTNAME")final String hostname,
			CassandraClient cassandraClient,
			CassandraConfiguration cassandraConfiguration,
			HectorConfiguration configuration,
			KairosDataPointFactory kairosDataPointFactory) throws DatastoreException
	{
		m_cassandraClient = cassandraClient;
		m_kairosDataPointFactory = kairosDataPointFactory;

		setupSchema();

		m_session = m_cassandraClient.getKeyspaceSession();
		//Prepare queries

		m_psInsertData = m_session.prepare(DATA_POINTS_INSERT);
		m_psInsertRowKey = m_session.prepare(ROW_KEY_INDEX_INSERT);
		m_psInsertString = m_session.prepare(STRING_INDEX_INSERT);

		try
		{
			m_cassandraConfiguration = cassandraConfiguration;
			m_singleRowReadSize = m_cassandraConfiguration.getSingleRowReadSize();
			m_multiRowSize = m_cassandraConfiguration.getMultiRowSize();
			m_multiRowReadSize = m_cassandraConfiguration.getMultiRowReadSize();
			m_keyspaceName = m_cassandraConfiguration.getKeyspaceName();

			m_rowKeyCache = new DataCache<DataPointsRowKey>(m_cassandraConfiguration.getRowKeyCacheSize());
			m_metricNameCache = new DataCache<String>(m_cassandraConfiguration.getStringCacheSize());
			m_tagNameCache = new DataCache<String>(m_cassandraConfiguration.getStringCacheSize());
			m_tagValueCache = new DataCache<String>(m_cassandraConfiguration.getStringCacheSize());

			CassandraHostConfigurator hostConfig = configuration.getConfiguration();
			int threadCount = hostConfig.buildCassandraHosts().length +3;

			m_cluster = HFactory.getOrCreateCluster("kairosdb-cluster",
					hostConfig, m_cassandraConfiguration.getCassandraAuthentication());

			KeyspaceDefinition keyspaceDef = m_cluster.describeKeyspace(m_keyspaceName);

			if (keyspaceDef == null) {
				createSchema(m_cassandraConfiguration.getReplicationFactor());
			}

			//set global consistency level
			ConfigurableConsistencyLevel confConsLevel = new ConfigurableConsistencyLevel();
			confConsLevel.setDefaultReadConsistencyLevel(m_cassandraConfiguration.getDataReadLevel().getHectorLevel());
			confConsLevel.setDefaultWriteConsistencyLevel(m_cassandraConfiguration.getDataWriteLevel().getHectorLevel());

			//create keyspace instance with specified consistency
			m_keyspace = HFactory.createKeyspace(m_keyspaceName, m_cluster, confConsLevel);

			ReentrantLock mutatorLock = new ReentrantLock();
			Condition lockCondition = mutatorLock.newCondition();

			/*m_dataPointWriteBuffer = new WriteBuffer<DataPointsRowKey, Integer, byte[]>(
					m_keyspace, CF_DATA_POINTS, m_cassandraConfiguration.getWriteDelay(),
					m_cassandraConfiguration.getMaxWriteSize(),
					DATA_POINTS_ROW_KEY_SERIALIZER,
					IntegerSerializer.get(),
					BytesArraySerializer.get(),
					createWriteBufferStats(CF_DATA_POINTS, hostname),
					mutatorLock, lockCondition, threadCount);*/

			/*m_rowKeyWriteBuffer = new WriteBuffer<String, DataPointsRowKey, String>(
					m_keyspace, CF_ROW_KEY_INDEX, m_cassandraConfiguration.getWriteDelay(),
					m_cassandraConfiguration.getMaxWriteSize(),
					StringSerializer.get(),
					DATA_POINTS_ROW_KEY_SERIALIZER,
					StringSerializer.get(),
					createWriteBufferStats(CF_ROW_KEY_INDEX, hostname),
					mutatorLock, lockCondition, threadCount);*/

			/*m_stringIndexWriteBuffer = new WriteBuffer<String, String, String>(
					m_keyspace, CF_STRING_INDEX,
					m_cassandraConfiguration.getWriteDelay(),
					m_cassandraConfiguration.getMaxWriteSize(),
					StringSerializer.get(),
					StringSerializer.get(),
					StringSerializer.get(),
					createWriteBufferStats(CF_STRING_INDEX, hostname),
					mutatorLock, lockCondition, threadCount);*/
		}
		catch (HectorException e)
		{
			throw new DatastoreException(e);
		}
	}

	private WriteBufferStats createWriteBufferStats(final String cfName, final String hostname) {
		return new WriteBufferStats()
		{
			private ImmutableSortedMap m_tags;
			{
				m_tags = ImmutableSortedMap.naturalOrder()
						.put("host", hostname)
						.put("buffer", cfName)
						.build();
			}

			@Override
			public void saveWriteSize(int pendingWrites)
			{
				putInternalDataPoint("kairosdb.datastore.write_size", m_tags,
						m_longDataPointFactory.createDataPoint(System.currentTimeMillis(), pendingWrites));
			}
		};
	}

	private void putInternalDataPoint(String metricName, ImmutableSortedMap<String, String> tags, DataPoint dataPoint)
	{
		try
		{
			putDataPoint(metricName, tags, dataPoint, 0);
		}
		catch (DatastoreException e)
		{
			logger.error("", e);
		}
	}


	private void setupSchema()
	{
		try (Session session = m_cassandraClient.getSession())
		{
			session.execute(String.format(CREATE_KEYSPACE, m_cassandraClient.getKeyspace()));
		}

		try (Session session = m_cassandraClient.getKeyspaceSession())
		{
			session.execute(DATA_POINTS_TABLE);
			session.execute(ROW_KEY_INDEX_TABLE);
			session.execute(STRING_INDEX_TABLE);
		}


	}

	private void createSchema(int replicationFactor)
	{
		/*
		CREATE TABLE data_points (
		  key blob,
		  column1 blob,
		  value blob,
		  PRIMARY KEY ((key), column1)
		) WITH COMPACT STORAGE AND
		  bloom_filter_fp_chance=0.010000 AND
		  caching='KEYS_ONLY' AND
		  comment='' AND
		  dclocal_read_repair_chance=0.100000 AND
		  gc_grace_seconds=864000 AND
		  index_interval=128 AND
		  read_repair_chance=1.000000 AND
		  replicate_on_write='true' AND
		  populate_io_cache_on_flush='false' AND
		  default_time_to_live=0 AND
		  speculative_retry='NONE' AND
		  memtable_flush_period_in_ms=0 AND
		  compaction={'timestamp_resolution': 'MILLISECONDS', 'max_sstable_age_days': '7', 'base_time_seconds': '600', 'class': 'DateTieredCompactionStrategy'} AND
		  compression={'sstable_compression': 'LZ4Compressor'};

      CREATE TABLE row_key_index (
		  key blob,
		  column1 blob,
		  value blob,
		  PRIMARY KEY ((key), column1)
		) WITH COMPACT STORAGE AND
		  bloom_filter_fp_chance=0.100000 AND
		  caching='KEYS_ONLY' AND
		  comment='' AND
		  dclocal_read_repair_chance=0.100000 AND
		  gc_grace_seconds=864000 AND
		  index_interval=128 AND
		  read_repair_chance=1.000000 AND
		  replicate_on_write='true' AND
		  populate_io_cache_on_flush='false' AND
		  default_time_to_live=0 AND
		  speculative_retry='NONE' AND
		  memtable_flush_period_in_ms=0 AND
		  compaction={'class': 'LeveledCompactionStrategy'} AND
		  compression={'sstable_compression': 'LZ4Compressor'};

		CREATE TABLE string_index (
		  key blob,
		  column1 text,
		  value blob,
		  PRIMARY KEY ((key), column1)
		) WITH COMPACT STORAGE AND
		  bloom_filter_fp_chance=0.100000 AND
		  caching='KEYS_ONLY' AND
		  comment='' AND
		  dclocal_read_repair_chance=0.100000 AND
		  gc_grace_seconds=864000 AND
		  index_interval=128 AND
		  read_repair_chance=1.000000 AND
		  replicate_on_write='true' AND
		  populate_io_cache_on_flush='false' AND
		  default_time_to_live=0 AND
		  speculative_retry='NONE' AND
		  memtable_flush_period_in_ms=0 AND
		  compaction={'class': 'LeveledCompactionStrategy'} AND
		  compression={'sstable_compression': 'LZ4Compressor'};


		 */
		/*List<ColumnFamilyDefinition> cfDef = new ArrayList<ColumnFamilyDefinition>();

		cfDef.add(HFactory.createColumnFamilyDefinition(
				m_keyspaceName, CF_DATA_POINTS, ComparatorType.BYTESTYPE));

		cfDef.add(HFactory.createColumnFamilyDefinition(
				m_keyspaceName, CF_ROW_KEY_INDEX, ComparatorType.BYTESTYPE));

		cfDef.add(HFactory.createColumnFamilyDefinition(
				m_keyspaceName, CF_STRING_INDEX, ComparatorType.UTF8TYPE));

		KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(
				m_keyspaceName, ThriftKsDef.DEF_STRATEGY_CLASS,
				replicationFactor, cfDef);

		m_cluster.addKeyspace(newKeyspace, true);*/
	}

	public void increaseMaxBufferSizes()
	{
		/*m_dataPointWriteBuffer.increaseMaxBufferSize();
		m_rowKeyWriteBuffer.increaseMaxBufferSize();
		m_stringIndexWriteBuffer.increaseMaxBufferSize();*/
	}

	public void cleanRowKeyCache()
	{
		long currentRow = calculateRowTime(System.currentTimeMillis());

		Set<DataPointsRowKey> keys = m_rowKeyCache.getCachedKeys();

		for (DataPointsRowKey key : keys)
		{
			if (key.getTimestamp() != currentRow)
			{
				m_rowKeyCache.removeKey(key);
			}
		}
	}

	@Override
	public void close() throws InterruptedException
	{
		/*m_dataPointWriteBuffer.close();
		m_rowKeyWriteBuffer.close();
		m_stringIndexWriteBuffer.close();*/

		m_session.close();
		m_cassandraClient.close();
	}

	@Override
	public void putDataPoint(String metricName,
			ImmutableSortedMap<String, String> tags,
			DataPoint dataPoint,
			int ttl) throws DatastoreException
	{
		try
		{
			DataPointsRowKey rowKey = null;
			//time the data is written.
			long writeTime = System.currentTimeMillis();
			if (0 == ttl)
				ttl = m_cassandraConfiguration.getDatapointTtl();

			int rowKeyTtl = 0;
			//Row key will expire 3 weeks after the data in the row expires
			if (ttl != 0)
				rowKeyTtl = ttl + ((int)(ROW_WIDTH / 1000));

			long rowTime= calculateRowTime(dataPoint.getTimestamp());

			rowKey = new DataPointsRowKey(metricName, rowTime, dataPoint.getDataStoreDataType(),
					tags);

			long now = System.currentTimeMillis();

			//Write out the row key if it is not cached
			DataPointsRowKey cachedKey = m_rowKeyCache.cacheItem(rowKey);
			if (cachedKey == null)
			{
				BoundStatement bs = new BoundStatement(m_psInsertRowKey);
				bs.setBytes(0, ByteBuffer.wrap(metricName.getBytes(UTF_8)));
				bs.setBytes(1, DATA_POINTS_ROW_KEY_SERIALIZER.toByteBuffer(rowKey));
				bs.setInt(2, rowKeyTtl);
				m_session.executeAsync(bs);

				/*m_rowKeyWriteBuffer.addData(metricName, rowKey, "", now, rowKeyTtl);*/
				for (RowKeyListener rowKeyListener : m_rowKeyListeners)
					rowKeyListener.addRowKey(metricName, rowKey, rowKeyTtl);
			}
			else
				rowKey = cachedKey;

			//Write metric name if not in cache
			String cachedName = m_metricNameCache.cacheItem(metricName);
			if (cachedName == null)
			{
				if (metricName.length() == 0)
				{
					logger.warn(
							"Attempted to add empty metric name to string index. Row looks like: "+dataPoint
					);
				}
				BoundStatement bs = new BoundStatement(m_psInsertString);
				bs.setBytes(0, ByteBuffer.wrap(ROW_KEY_METRIC_NAMES.getBytes(UTF_8)));
				bs.setString(1, metricName);
				m_session.executeAsync(bs);
				/*m_stringIndexWriteBuffer.addData(ROW_KEY_METRIC_NAMES,
						metricName, "", now);*/
			}

			//Check tag names and values to write them out
			for (String tagName : tags.keySet())
			{
				String cachedTagName = m_tagNameCache.cacheItem(tagName);
				if (cachedTagName == null)
				{
					if(tagName.length() == 0)
					{
						logger.warn(
								"Attempted to add empty tagName to string cache for metric: "+metricName
						);
					}
					BoundStatement bs = new BoundStatement(m_psInsertString);
					bs.setBytes(0, ByteBuffer.wrap(ROW_KEY_TAG_NAMES.getBytes(UTF_8)));
					bs.setString(1, tagName);
					m_session.executeAsync(bs);
					/*m_stringIndexWriteBuffer.addData(ROW_KEY_TAG_NAMES,
							tagName, "", now);*/

				}

				String value = tags.get(tagName);
				String cachedValue = m_tagValueCache.cacheItem(value);
				if (cachedValue == null)
				{
					if(value.toString().length() == 0)
					{
						logger.warn(
								"Attempted to add empty tagValue (tag name "+tagName+") to string cache for metric: "+metricName
						);
					}
					BoundStatement bs = new BoundStatement(m_psInsertString);
					bs.setBytes(0, ByteBuffer.wrap(ROW_KEY_TAG_VALUES.getBytes(UTF_8)));
					bs.setString(1, value);
					m_session.executeAsync(bs);
					/*m_stringIndexWriteBuffer.addData(ROW_KEY_TAG_VALUES,
							value, "", now);*/
				}
			}

			int columnTime = getColumnName(rowTime, dataPoint.getTimestamp());
			KDataOutput kDataOutput = new KDataOutput();
			dataPoint.writeValueToBuffer(kDataOutput);
			/*m_dataPointWriteBuffer.addData(rowKey, columnTime,
					kDataOutput.getBytes(), writeTime, ttl);*/


			BoundStatement boundStatement = new BoundStatement(m_psInsertData);
			boundStatement.setBytes(0, DATA_POINTS_ROW_KEY_SERIALIZER.toByteBuffer(rowKey));
			ByteBuffer b = ByteBuffer.allocate(4);
			b.putInt(columnTime);
			b.rewind();
			boundStatement.setBytes(1, b);
			boundStatement.setBytes(2, ByteBuffer.wrap(kDataOutput.getBytes()));
			boundStatement.setInt(3, ttl);
			//m_session.executeAsync(boundStatement);

			if (m_batchStatement == null)
				m_batchStatement = new BatchStatement();

			m_batchStatement.add(boundStatement);

			if (m_batchStatement.size() > 390)
			{
				synchronized (m_batchLock)
				{
					m_session.executeAsync(m_batchStatement);
					m_batchStatement = new BatchStatement();
				}
			}

		}
		catch (Exception e)
		{
			throw new DatastoreException(e);
		}
	}

	private Iterable<String> queryStringIndex(final String key) {
		SliceQuery<String, String, String> sliceQuery =
				HFactory.createSliceQuery(m_keyspace, StringSerializer.get(), StringSerializer.get(),
						StringSerializer.get());

		sliceQuery.setColumnFamily(CF_STRING_INDEX);
		sliceQuery.setKey(key);

		ColumnSliceIterator<String, String, String> columnIterator =
				new ColumnSliceIterator<String, String, String>(sliceQuery, "", (String) null, false, m_singleRowReadSize);

		List<String> ret = new ArrayList<String>();

		while (columnIterator.hasNext()) {
			ret.add(columnIterator.next().getName());
		}

		return ret;
	}

	@Override
	public Iterable<String> getMetricNames()
	{
		return queryStringIndex(ROW_KEY_METRIC_NAMES);
	}

	@Override
	public Iterable<String> getTagNames()
	{
		return queryStringIndex(ROW_KEY_TAG_NAMES);
	}

	@Override
	public Iterable<String> getTagValues()
	{
		return queryStringIndex(ROW_KEY_TAG_VALUES);
	}

	@Override
	public TagSet queryMetricTags(DatastoreMetricQuery query)
	{
		TagSetImpl tagSet = new TagSetImpl();
		Iterator<DataPointsRowKey> rowKeys = getKeysForQueryIterator(query);

		MemoryMonitor mm = new MemoryMonitor(20);
		while (rowKeys.hasNext())
		{
			DataPointsRowKey dataPointsRowKey = rowKeys.next();
			for (Map.Entry<String, String> tag : dataPointsRowKey.getTags().entrySet())
			{
				tagSet.addTag(tag.getKey(), tag.getValue());
				mm.checkMemoryAndThrowException();
			}
		}

		return (tagSet);
	}

	@Override
	public void queryDatabase(DatastoreMetricQuery query, QueryCallback queryCallback)
	{
		queryWithRowKeys(query, queryCallback, getKeysForQueryIterator(query));
	}

	private void queryWithRowKeys(DatastoreMetricQuery query,
			QueryCallback queryCallback, Iterator<DataPointsRowKey> rowKeys)
	{
		long startTime = System.currentTimeMillis();
		long currentTimeTier = 0L;
		String currentType = null;

		List<QueryRunner> runners = new ArrayList<QueryRunner>();
		List<DataPointsRowKey> queryKeys = new ArrayList<DataPointsRowKey>();

		MemoryMonitor mm = new MemoryMonitor(20);
		while (rowKeys.hasNext())
		{
			DataPointsRowKey rowKey = rowKeys.next();
			if (currentTimeTier == 0L)
				currentTimeTier = rowKey.getTimestamp();

			if (currentType == null)
				currentType = rowKey.getDataType();

			if ((rowKey.getTimestamp() == currentTimeTier) && (queryKeys.size() < m_multiRowSize) &&
					(currentType.equals(rowKey.getDataType())))
			{
				queryKeys.add(rowKey);
			}
			else
			{
				runners.add(new QueryRunner(m_keyspace, CF_DATA_POINTS, m_kairosDataPointFactory,
						queryKeys,
						query.getStartTime(), query.getEndTime(), queryCallback, m_singleRowReadSize,
						m_multiRowReadSize, query.getLimit(), query.getOrder()));

				queryKeys = new ArrayList<DataPointsRowKey>();
				queryKeys.add(rowKey);
				currentTimeTier = rowKey.getTimestamp();
				currentType = rowKey.getDataType();
			}

			mm.checkMemoryAndThrowException();
		}

		//There may be stragglers that are not ran
		if (!queryKeys.isEmpty())
		{
			runners.add(new QueryRunner(m_keyspace, CF_DATA_POINTS, m_kairosDataPointFactory,
					queryKeys,
					query.getStartTime(), query.getEndTime(), queryCallback, m_singleRowReadSize,
					m_multiRowReadSize, query.getLimit(), query.getOrder()));
		}

		ThreadReporter.addDataPoint(KEY_QUERY_TIME, System.currentTimeMillis() - startTime);

		//Changing the check rate
		mm.setCheckRate(1);
		try
		{
			//TODO: Run this with multiple threads
			for (QueryRunner runner : runners)
			{
				runner.runQuery();

				mm.checkMemoryAndThrowException();
			}

			queryCallback.endDataPoints();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	@Override
	public void deleteDataPoints(DatastoreMetricQuery deleteQuery) throws DatastoreException
	{
		checkNotNull(deleteQuery);

		long now = System.currentTimeMillis();

		boolean deleteAll = false;
		if (deleteQuery.getStartTime() == Long.MIN_VALUE && deleteQuery.getEndTime() == Long.MAX_VALUE)
			deleteAll = true;

		Iterator<DataPointsRowKey> rowKeyIterator = getKeysForQueryIterator(deleteQuery);
		List<DataPointsRowKey> partialRows = new ArrayList<DataPointsRowKey>();

		while (rowKeyIterator.hasNext())
		{
			DataPointsRowKey rowKey = rowKeyIterator.next();
			long rowKeyTimestamp = rowKey.getTimestamp();
			if (deleteQuery.getStartTime() <= rowKeyTimestamp && (deleteQuery.getEndTime() >= rowKeyTimestamp + ROW_WIDTH - 1))
			{
				//todo fix me
				//m_dataPointWriteBuffer.deleteRow(rowKey, now);  // delete the whole row
				//m_rowKeyWriteBuffer.deleteColumn(rowKey.getMetricName(), rowKey, now); // Delete the index
				m_rowKeyCache.clear();
			}
			else
			{
				partialRows.add(rowKey);
			}
		}

		queryWithRowKeys(deleteQuery, new DeletingCallback(deleteQuery.getName()), partialRows.iterator());

		// If index is gone, delete metric name from Strings column family
		if (deleteAll)
		{
			//m_rowKeyWriteBuffer.deleteRow(deleteQuery.getName(), now);
			//todo fix me
			//m_stringIndexWriteBuffer.deleteColumn(ROW_KEY_METRIC_NAMES, deleteQuery.getName(), now);
			m_rowKeyCache.clear();
			m_metricNameCache.clear();
		}
	}

	private SortedMap<String, String> getTags(DataPointRow row)
	{
		TreeMap<String, String> map = new TreeMap<String, String>();
		for (String name : row.getTagNames())
		{
			map.put(name, row.getTagValue(name));
		}

		return map;
	}

	/**
	 * Returns the row keys for the query in tiers ie grouped by row key timestamp
	 *
	 * @param query query
	 * @return row keys for the query
	 */
	public Iterator<DataPointsRowKey> getKeysForQueryIterator(DatastoreMetricQuery query)
	{
		Iterator<DataPointsRowKey> ret = null;

		List<QueryPlugin> plugins = query.getPlugins();

		//First plugin that works gets it.
		for (QueryPlugin plugin : plugins)
		{
			if (plugin instanceof CassandraRowKeyPlugin)
			{
				ret = ((CassandraRowKeyPlugin) plugin).getKeysForQueryIterator(query);
				break;
			}
		}

		//Default to old behavior if no plugin was provided
		if (ret == null)
		{
			ret = new FilteredRowKeyIterator(query.getName(), query.getStartTime(),
					query.getEndTime(), query.getTags());
		}

		return (ret);
	}

	public static long calculateRowTime(long timestamp)
	{
		return (timestamp - (Math.abs(timestamp) % ROW_WIDTH));
	}


	/**
	 This is just for the delete operation of old data points.
	 @param rowTime
	 @param timestamp
	 @param isInteger
	 @return
	 */
	@SuppressWarnings("PointlessBitwiseExpression")
	private static int getColumnName(long rowTime, long timestamp, boolean isInteger)
	{
		int ret = (int) (timestamp - rowTime);

		if (isInteger)
			return ((ret << 1) | LONG_FLAG);
		else
			return ((ret << 1) | FLOAT_FLAG);

	}

	@SuppressWarnings("PointlessBitwiseExpression")
	public static int getColumnName(long rowTime, long timestamp)
	{
		int ret = (int) (timestamp - rowTime);

		/*
			The timestamp is shifted to support legacy datapoints that
			used the extra bit to determine if the value was long or double
		 */
		return (ret << 1);
	}

	public static long getColumnTimestamp(long rowTime, int columnName)
	{
		return (rowTime + (long) (columnName >>> 1));
	}

	public static boolean isLongValue(int columnName)
	{
		return ((columnName & 0x1) == LONG_FLAG);
	}

	private class FilteredRowKeyIterator implements Iterator<DataPointsRowKey>
	{
		private ColumnSliceIterator<String, DataPointsRowKey, String> m_sliceIterator;

		/**
		 Used when a query spans positive and negative time values, we have to
		 query the positive separate from the negative times as negative times
		 are sorted after the positive ones.
		 */
		private ColumnSliceIterator<String, DataPointsRowKey, String> m_continueSliceIterator;
		private DataPointsRowKey m_nextKey;
		private SetMultimap<String, String> m_filterTags;

		public FilteredRowKeyIterator(String metricName, long startTime, long endTime,
				SetMultimap<String, String> filterTags)
		{
			m_filterTags = filterTags;
			SliceQuery<String, DataPointsRowKey, String> sliceQuery =
					HFactory.createSliceQuery(m_keyspace, StringSerializer.get(),
							new DataPointsRowKeySerializer(true), StringSerializer.get());

			sliceQuery.setColumnFamily(CF_ROW_KEY_INDEX)
					.setKey(metricName);

			if ((startTime < 0) && (endTime >= 0))
			{
				m_sliceIterator = createSliceIterator(sliceQuery, metricName,
						startTime, -1L);

				SliceQuery<String, DataPointsRowKey, String> sliceQuery2 =
						HFactory.createSliceQuery(m_keyspace, StringSerializer.get(),
								new DataPointsRowKeySerializer(true), StringSerializer.get());

				sliceQuery2.setColumnFamily(CF_ROW_KEY_INDEX)
						.setKey(metricName);

				m_continueSliceIterator = createSliceIterator(sliceQuery2, metricName,
						0, endTime);
			}
			else
			{
				m_sliceIterator = createSliceIterator(sliceQuery, metricName,
						startTime, endTime);
			}

		}

		private ColumnSliceIterator<String, DataPointsRowKey, String> createSliceIterator(
				SliceQuery<String, DataPointsRowKey, String> sliceQuery,
				String metricName, long startTime, long endTime)
		{
			DataPointsRowKey startKey = new DataPointsRowKey(metricName,
					calculateRowTime(startTime), "");

			DataPointsRowKey endKey = new DataPointsRowKey(metricName,
					calculateRowTime(endTime), "");
			endKey.setEndSearchKey(true);

			ColumnSliceIterator<String, DataPointsRowKey, String> iterator = new ColumnSliceIterator<String, DataPointsRowKey, String>(sliceQuery,
					startKey, endKey, false, m_singleRowReadSize);

			return (iterator);
		}

		private DataPointsRowKey nextKeyFromIterator(ColumnSliceIterator<String, DataPointsRowKey, String> iterator)
		{
			DataPointsRowKey next = null;

			outer:
			while (iterator.hasNext())
			{
				DataPointsRowKey rowKey = iterator.next().getName();

				Map<String, String> keyTags = rowKey.getTags();
				for (String tag : m_filterTags.keySet())
				{
					String value = keyTags.get(tag);
					if (value == null || !m_filterTags.get(tag).contains(value))
						continue outer; //Don't want this key
				}

				next = rowKey;
				break;
			}

			return (next);
		}

		@Override
		public boolean hasNext()
		{
			m_nextKey = nextKeyFromIterator(m_sliceIterator);

			if ((m_nextKey == null) && (m_continueSliceIterator != null))
				m_nextKey = nextKeyFromIterator(m_continueSliceIterator);

			return (m_nextKey != null);
		}

		@Override
		public DataPointsRowKey next()
		{
			return m_nextKey;
		}

		@Override
		public void remove()
		{
		}
	}

	private class DeletingCallback implements QueryCallback
	{
		private SortedMap<String, String> m_currentTags;
		private DataPointsRowKey m_currentRow;
		private long m_now = System.currentTimeMillis();
		private final String m_metric;
		private String m_currentType;

		public DeletingCallback(String metric)
		{
			m_metric = metric;
		}


		@Override
		public void addDataPoint(DataPoint datapoint) throws IOException
		{
			long time = datapoint.getTimestamp();

			long rowTime = calculateRowTime(time);
			if (m_currentRow == null)
			{
				m_currentRow = new DataPointsRowKey(m_metric, rowTime, m_currentType, m_currentTags);
			}

			int columnName;
			//Handle old column name format.
			//We get the type after it has been translated from "" to kairos_legacy
			if (m_currentType.equals(LegacyDataPointFactory.DATASTORE_TYPE))
			{
				columnName = getColumnName(rowTime, time, datapoint.isLong());
			}
			else
				columnName = getColumnName(rowTime, time);

			//todo fix me
			//m_dataPointWriteBuffer.deleteColumn(m_currentRow, columnName, m_now);
		}

		@Override
		public void startDataPointSet(String dataType, Map<String, String> tags) throws IOException
		{
			m_currentType = dataType;
			m_currentTags = new TreeMap<String, String>(tags);
			//This causes the row key to get reset with the first data point
			m_currentRow = null;
		}

		@Override
		public void endDataPoints()
		{
		}
	}
}
