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

import com.google.common.collect.SetMultimap;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.CountQuery;
import me.prettyprint.hector.api.query.SliceQuery;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;
import org.kairosdb.core.datastore.*;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.reporting.ThreadReporter;
import org.kairosdb.util.MemoryMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.kairosdb.core.CoreModule.DATAPOINTS_FACTORY_LONG;

public class CassandraDatastore implements Datastore
{
	public static final Logger logger = LoggerFactory.getLogger(CassandraDatastore.class);

	public static final int ROW_KEY_CACHE_SIZE = 1024;
	public static final int STRING_CACHE_SIZE = 1024;

	public static final int LONG_FLAG = 0x0;
	public static final int FLOAT_FLAG = 0x1;

	public static final DataPointsRowKeySerializer DATA_POINTS_ROW_KEY_SERIALIZER = new DataPointsRowKeySerializer();

	public static final String REPLICATION_FACTOR_PROPERTY = "kairosdb.datastore.cassandra.replication_factor";
	public static final long ROW_WIDTH = 1814400000L; //3 Weeks wide
	public static final String WRITE_DELAY_PROPERTY = "kairosdb.datastore.cassandra.write_delay";
	public static final String KEY_QUERY_TIME = "kairosdb.datastore.cassandra.key_query_time";
	public static final String WRITE_BUFFER_SIZE = "kairosdb.datastore.cassandra.write_buffer_max_size";
	public static final String SINGLE_ROW_READ_SIZE_PROPERTY = "kairosdb.datastore.cassandra.single_row_read_size";
	public static final String MULTI_ROW_READ_SIZE_PROPERTY = "kairosdb.datastore.cassandra.multi_row_read_size";
	public static final String MULTI_ROW_SIZE_PROPERTY = "kairosdb.datastore.cassandra.multi_row_size";
	public static final String DATA_READ_CONSISTENCY_LEVEL = "kairosdb.datastore.cassandra.data_read_consistency_level";
	public static final String DATA_WRITE_CONSISTENCY_LEVEL = "kairosdb.datastore.cassandra.data_write_consistency_level";
	public static final String INDEX_READ_CONSISTENCY_LEVEL = "kairosdb.datastore.cassandra.index_read_consistency_level";
	public static final String INDEX_WRITE_CONSISTENCY_LEVEL = "kairosdb.datastore.cassandra.index_write_consistency_level";

	public static final String KEYSPACE = "kairosdb";
	public static final String CF_DATA_POINTS = "data_points";
	public static final String CF_ROW_KEY_INDEX = "row_key_index";
	public static final String CF_STRING_INDEX = "string_index";

	public static final String ROW_KEY_METRIC_NAMES = "metric_names";
	public static final String ROW_KEY_TAG_NAMES = "tag_names";
	public static final String ROW_KEY_TAG_VALUES = "tag_values";


	private Cluster m_cluster;
	private Keyspace m_keyspace;
	private int m_singleRowReadSize;
	private int m_multiRowSize;
	private int m_multiRowReadSize;
	private WriteBuffer<DataPointsRowKey, Integer, ByteBuffer> m_dataPointWriteBuffer;
	private WriteBuffer<String, DataPointsRowKey, String> m_rowKeyWriteBuffer;
	private WriteBuffer<String, String, String> m_stringIndexWriteBuffer;

	private DataCache<DataPointsRowKey> m_rowKeyCache = new DataCache<DataPointsRowKey>(ROW_KEY_CACHE_SIZE);
	private DataCache<String> m_metricNameCache = new DataCache<String>(STRING_CACHE_SIZE);
	private DataCache<String> m_tagNameCache = new DataCache<String>(STRING_CACHE_SIZE);
	private DataCache<String> m_tagValueCache = new DataCache<String>(STRING_CACHE_SIZE);

	@Inject
	private LongDataPointFactory m_longDataPointFactory = new LongDataPointFactoryImpl();

	@Inject
	@Named(DATA_WRITE_CONSISTENCY_LEVEL)
	private ConsitencyLevel m_dataWriteLevel = ConsitencyLevel.QUORUM;

	@Inject
	@Named(DATA_READ_CONSISTENCY_LEVEL)
	private ConsitencyLevel m_dataReadLevel = ConsitencyLevel.ONE;

	@Inject
	@Named(INDEX_WRITE_CONSISTENCY_LEVEL)
	private ConsitencyLevel m_indexWriteLevel = ConsitencyLevel.QUORUM;

	@Inject
	@Named(INDEX_READ_CONSISTENCY_LEVEL)
	private ConsitencyLevel m_indexReadLevel = ConsitencyLevel.ONE;


	@Inject
	public CassandraDatastore(@Named(CassandraModule.CASSANDRA_AUTH_MAP) Map<String, String> cassandraAuthentication,
	                          @Named(REPLICATION_FACTOR_PROPERTY) int replicationFactor,
	                          @Named(SINGLE_ROW_READ_SIZE_PROPERTY) int singleRowReadSize,
	                          @Named(MULTI_ROW_SIZE_PROPERTY) int multiRowSize,
	                          @Named(MULTI_ROW_READ_SIZE_PROPERTY) int multiRowReadSize,
	                          @Named(WRITE_DELAY_PROPERTY) int writeDelay,
	                          @Named(WRITE_BUFFER_SIZE) int maxWriteSize,
	                          final @Named("HOSTNAME") String hostname,
	                          HectorConfiguration configuration) throws DatastoreException
	{
		try
		{
			m_singleRowReadSize = singleRowReadSize;
			m_multiRowSize = multiRowSize;
			m_multiRowReadSize = multiRowReadSize;

			CassandraHostConfigurator hostConfig = configuration.getConfiguration();

			m_cluster = HFactory.getOrCreateCluster("kairosdb-cluster",
					hostConfig, cassandraAuthentication);

			KeyspaceDefinition keyspaceDef = m_cluster.describeKeyspace(KEYSPACE);

			if (keyspaceDef == null)
				createSchema(replicationFactor);

			ConfigurableConsistencyLevel confConsLevel = new ConfigurableConsistencyLevel();

			Map<String, HConsistencyLevel> readLevels = new HashMap<String, HConsistencyLevel>();
			readLevels.put(CF_DATA_POINTS, m_dataReadLevel.getHectorLevel());
			readLevels.put(CF_ROW_KEY_INDEX, m_indexReadLevel.getHectorLevel());

			Map <String, HConsistencyLevel> writeLevels = new HashMap<String, HConsistencyLevel>();
			writeLevels.put(CF_DATA_POINTS, m_dataWriteLevel.getHectorLevel());
			writeLevels.put(CF_ROW_KEY_INDEX, m_indexWriteLevel.getHectorLevel());

			confConsLevel.setReadCfConsistencyLevels(readLevels);
			confConsLevel.setWriteCfConsistencyLevels(writeLevels);

			m_keyspace = HFactory.createKeyspace(KEYSPACE, m_cluster, confConsLevel);

			ReentrantLock mutatorLock = new ReentrantLock();
			Condition lockCondition = mutatorLock.newCondition();

			m_dataPointWriteBuffer = new WriteBuffer<DataPointsRowKey, Integer, ByteBuffer>(
					m_keyspace, CF_DATA_POINTS, writeDelay, maxWriteSize,
					DATA_POINTS_ROW_KEY_SERIALIZER,
					IntegerSerializer.get(),
					ByteBufferSerializer.get(),
					new WriteBufferStats()
					{
						@Override
						public void saveWriteSize(int pendingWrites)
						{
							DataPointSet dps = new DataPointSet("kairosdb.datastore.write_size");
							dps.addTag("host", hostname);
							dps.addTag("buffer", CF_DATA_POINTS);
							dps.addDataPoint(m_longDataPointFactory.createDataPoint(System.currentTimeMillis(), pendingWrites));
							putInternalDataPoints(dps);
						}
					}, mutatorLock, lockCondition);

			m_rowKeyWriteBuffer = new WriteBuffer<String, DataPointsRowKey, String>(
					m_keyspace, CF_ROW_KEY_INDEX, writeDelay, maxWriteSize,
					StringSerializer.get(),
					DATA_POINTS_ROW_KEY_SERIALIZER,
					StringSerializer.get(),
					new WriteBufferStats()
					{
						@Override
						public void saveWriteSize(int pendingWrites)
						{
							DataPointSet dps = new DataPointSet("kairosdb.datastore.write_size");
							dps.addTag("host", hostname);
							dps.addTag("buffer", CF_ROW_KEY_INDEX);
							dps.addDataPoint(m_longDataPointFactory.createDataPoint(System.currentTimeMillis(), pendingWrites));
							putInternalDataPoints(dps);
						}
					}, mutatorLock, lockCondition);

			m_stringIndexWriteBuffer = new WriteBuffer<String, String, String>(
					m_keyspace, CF_STRING_INDEX, writeDelay, maxWriteSize,
					StringSerializer.get(),
					StringSerializer.get(),
					StringSerializer.get(),
					new WriteBufferStats()
					{
						@Override
						public void saveWriteSize(int pendingWrites)
						{
							DataPointSet dps = new DataPointSet("kairosdb.datastore.write_size");
							dps.addTag("host", hostname);
							dps.addTag("buffer", CF_STRING_INDEX);
							dps.addDataPoint(m_longDataPointFactory.createDataPoint(System.currentTimeMillis(), pendingWrites));
							putInternalDataPoints(dps);
						}
					}, mutatorLock, lockCondition);
		}
		catch (HectorException e)
		{
			throw new DatastoreException(e);
		}
	}

	private void putInternalDataPoints(DataPointSet dps)
	{
		try
		{
			putDataPoints(dps);
		}
		catch (DatastoreException e)
		{
			logger.error("", e);
		}
	}

	private void createSchema(int replicationFactor)
	{
		List<ColumnFamilyDefinition> cfDef = new ArrayList<ColumnFamilyDefinition>();

		cfDef.add(HFactory.createColumnFamilyDefinition(
				KEYSPACE, CF_DATA_POINTS, ComparatorType.BYTESTYPE));

		cfDef.add(HFactory.createColumnFamilyDefinition(
				KEYSPACE, CF_ROW_KEY_INDEX, ComparatorType.BYTESTYPE));

		cfDef.add(HFactory.createColumnFamilyDefinition(
				KEYSPACE, CF_STRING_INDEX, ComparatorType.UTF8TYPE));

		KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(
				KEYSPACE, ThriftKsDef.DEF_STRATEGY_CLASS,
				replicationFactor, cfDef);

		m_cluster.addKeyspace(newKeyspace, true);
	}

	public void increaseMaxBufferSizes()
	{
		m_dataPointWriteBuffer.increaseMaxBufferSize();
		m_rowKeyWriteBuffer.increaseMaxBufferSize();
		m_stringIndexWriteBuffer.increaseMaxBufferSize();
	}

	@Override
	public void close() throws InterruptedException
	{
		m_dataPointWriteBuffer.close();
		m_rowKeyWriteBuffer.close();
		m_stringIndexWriteBuffer.close();
	}


	@Override
	public void putDataPoints(DataPointSet dps) throws DatastoreException
	{
		try
		{
			long rowTime = -1L;
			DataPointsRowKey rowKey = null;
			//time the data is written.
			long writeTime = System.currentTimeMillis();

			for (DataPoint dp : dps.getDataPoints())
			{
				if (dp.getTimestamp() < 0)
					throw new DatastoreException("Timestamp must be greater than or equal to zero.");
				long newRowTime = calculateRowTime(dp.getTimestamp());
				if (newRowTime != rowTime)
				{
					rowTime = newRowTime;
					rowKey = new DataPointsRowKey(dps.getName(), rowTime, dps.getDataStoreDataType(),
							dps.getTags());

					long now = System.currentTimeMillis();
					//Write out the row key if it is not cached
					if (!m_rowKeyCache.isCached(rowKey))
						m_rowKeyWriteBuffer.addData(dps.getName(), rowKey, "", now);

					//Write metric name if not in cache
					if (!m_metricNameCache.isCached(dps.getName()))
					{
						m_stringIndexWriteBuffer.addData(ROW_KEY_METRIC_NAMES,
								dps.getName(), "", now);
					}

					//Check tag names and values to write them out
					Map<String, String> tags = dps.getTags();
					for (String tagName : tags.keySet())
					{
						if (!m_tagNameCache.isCached(tagName))
						{
							m_stringIndexWriteBuffer.addData(ROW_KEY_TAG_NAMES,
									tagName, "", now);
						}

						String value = tags.get(tagName);
						if (!m_tagValueCache.isCached(value))
						{
							m_stringIndexWriteBuffer.addData(ROW_KEY_TAG_VALUES,
									value, "", now);
						}
					}
				}

				int columnTime = getColumnName(rowTime, dp.getTimestamp());
				m_dataPointWriteBuffer.addData(rowKey, columnTime,
						dp.toByteBuffer(), writeTime);

			}
		}
		catch (DatastoreException e)
		{
			throw e;
		}
		catch (Exception e)
		{
			throw new DatastoreException(e);
		}
	}

	@Override
	public Iterable<String> getMetricNames()
	{
		SliceQuery<String, String, String> sliceQuery =
				HFactory.createSliceQuery(m_keyspace, StringSerializer.get(), StringSerializer.get(),
						StringSerializer.get());

		sliceQuery.setColumnFamily(CF_STRING_INDEX);
		sliceQuery.setKey(ROW_KEY_METRIC_NAMES);

		ColumnSliceIterator<String, String, String> columnIterator =
				new ColumnSliceIterator<String, String, String>(sliceQuery, "", (String) null, false, m_singleRowReadSize);

		List<String> ret = new ArrayList<String>();

		while (columnIterator.hasNext())
			ret.add(columnIterator.next().getName());

		return (ret);
	}

	@Override
	public Iterable<String> getTagNames()
	{
		SliceQuery<String, String, String> sliceQuery =
				HFactory.createSliceQuery(m_keyspace, StringSerializer.get(), StringSerializer.get(),
						StringSerializer.get());

		sliceQuery.setColumnFamily(CF_STRING_INDEX);
		sliceQuery.setKey(ROW_KEY_TAG_NAMES);

		ColumnSliceIterator<String, String, String> columnIterator =
				new ColumnSliceIterator<String, String, String>(sliceQuery, "", (String) null, false, m_singleRowReadSize);

		List<String> ret = new ArrayList<String>();

		while (columnIterator.hasNext())
			ret.add(columnIterator.next().getName());

		return (ret);
	}

	@Override
	public Iterable<String> getTagValues()
	{
		SliceQuery<String, String, String> sliceQuery =
				HFactory.createSliceQuery(m_keyspace, StringSerializer.get(), StringSerializer.get(),
						StringSerializer.get());

		sliceQuery.setColumnFamily(CF_STRING_INDEX);
		sliceQuery.setKey(ROW_KEY_TAG_VALUES);

		ColumnSliceIterator<String, String, String> columnIterator =
				new ColumnSliceIterator<String, String, String>(sliceQuery, "", (String) null, false, m_singleRowReadSize);

		List<String> ret = new ArrayList<String>();

		while (columnIterator.hasNext())
			ret.add(columnIterator.next().getName());

		return (ret);
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

		List<QueryRunner> runners = new ArrayList<QueryRunner>();
		List<DataPointsRowKey> queryKeys = new ArrayList<DataPointsRowKey>();

		MemoryMonitor mm = new MemoryMonitor(20);
		while (rowKeys.hasNext())
		{
			DataPointsRowKey rowKey = rowKeys.next();
			if (currentTimeTier == 0L)
				currentTimeTier = rowKey.getTimestamp();

			if ((rowKey.getTimestamp() == currentTimeTier) && queryKeys.size() < m_multiRowSize)
			{
				queryKeys.add(rowKey);
			}
			else
			{
				runners.add(new QueryRunner(m_keyspace, CF_DATA_POINTS, queryKeys,
						query.getStartTime(), query.getEndTime(), queryCallback, m_singleRowReadSize,
						m_multiRowReadSize, query.getLimit(), query.getOrder()));

				queryKeys = new ArrayList<DataPointsRowKey>();
				queryKeys.add(rowKey);
				currentTimeTier = rowKey.getTimestamp();
			}

			mm.checkMemoryAndThrowException();
		}

		//There may be stragglers that are not ran
		if (!queryKeys.isEmpty())
		{
			runners.add(new QueryRunner(m_keyspace, CF_DATA_POINTS, queryKeys,
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

		// Get number of columns in the row key
		// We do this up front to avoid waiting for a sync of cassandra to see
		// if they are all gone.
		CountQuery<String, DataPointsRowKey> countQuery = HFactory.createCountQuery(m_keyspace, StringSerializer.get(), DATA_POINTS_ROW_KEY_SERIALIZER);
		countQuery.setColumnFamily(CF_ROW_KEY_INDEX).
				setKey(deleteQuery.getName()).
				setRange(new DataPointsRowKey(deleteQuery.getName(), 0L, ""), new DataPointsRowKey(deleteQuery.getName(), Long.MAX_VALUE, ""), Integer.MAX_VALUE);
		int rowKeyColumnCount = countQuery.execute().get();

		Iterator<DataPointsRowKey> rowKeyIterator = getKeysForQueryIterator(deleteQuery);
		List<DataPointsRowKey> partialRows = new ArrayList<DataPointsRowKey>();

		while (rowKeyIterator.hasNext())
		{
			DataPointsRowKey rowKey = rowKeyIterator.next();
			long rowKeyTimestamp = rowKey.getTimestamp();
			if (deleteQuery.getStartTime() <= rowKeyTimestamp && (deleteQuery.getEndTime() >= rowKeyTimestamp + ROW_WIDTH - 1))
			{
				m_dataPointWriteBuffer.deleteRow(rowKey, now);  // delete the whole row
				m_rowKeyWriteBuffer.deleteColumn(rowKey.getMetricName(), rowKey, now); // Delete the index
				m_rowKeyCache.clear();
				rowKeyColumnCount--;
			}
			else
			{
				if (rowKey.getDataType() == null)
				partialRows.add(rowKey);
			}
		}

		queryWithRowKeys(deleteQuery, new DeletingCallback(deleteQuery.getName()), partialRows.iterator());

		// If index is gone, delete metric name from Strings column family
		if (rowKeyColumnCount < 1)
		{
			m_rowKeyWriteBuffer.deleteRow(deleteQuery.getName(), now);
			m_stringIndexWriteBuffer.deleteColumn(ROW_KEY_METRIC_NAMES, deleteQuery.getName(), now);
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
	/*package*/ Iterator<DataPointsRowKey> getKeysForQueryIterator(DatastoreMetricQuery query)
	{
		return (new FilteredRowKeyIterator(query.getName(), query.getStartTime(),
				query.getEndTime(), query.getTags()));
	}

	public static long calculateRowTime(long timestamp)
	{
		return (timestamp - (timestamp % ROW_WIDTH));
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
		private DataPointsRowKey m_nextKey;
		private SetMultimap<String, String> m_filterTags;

		public FilteredRowKeyIterator(String metricName, long startTime, long endTime,
				SetMultimap<String, String> filterTags)
		{
			m_filterTags = filterTags;
			SliceQuery<String, DataPointsRowKey, String> sliceQuery =
					HFactory.createSliceQuery(m_keyspace, StringSerializer.get(),
							new DataPointsRowKeySerializer(true), StringSerializer.get());

			DataPointsRowKey startKey = new DataPointsRowKey(metricName,
					calculateRowTime(startTime), "");

			/*
			Adding 1 to the end time ensures we get all the keys that have end time and
			have tags in the key.
			 */
			DataPointsRowKey endKey = new DataPointsRowKey(metricName,
					calculateRowTime(endTime) + 1, "");


			sliceQuery.setColumnFamily(CF_ROW_KEY_INDEX)
					.setKey(metricName);

			m_sliceIterator =
					new ColumnSliceIterator<String, DataPointsRowKey, String>(sliceQuery,
							startKey, endKey, false, m_singleRowReadSize);

		}


		@Override
		public boolean hasNext()
		{
			m_nextKey = null;

			outer:
			while (m_sliceIterator.hasNext())
			{
				DataPointsRowKey rowKey = m_sliceIterator.next().getName();

				Map<String, String> keyTags = rowKey.getTags();
				for (String tag : m_filterTags.keySet())
				{
					String value = keyTags.get(tag);
					if (value == null || !m_filterTags.get(tag).contains(value))
						continue outer; //Don't want this key
				}

				m_nextKey = rowKey;
				break;
			}

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


		private void deleteDataPoint(long time, boolean isInteger)
		{
			long rowTime = calculateRowTime(time);
			if (m_currentRow == null)
			{
				m_currentRow = new DataPointsRowKey(m_metric, rowTime, m_currentType, m_currentTags);
			}

			int columnName;
			//Handle old column name format.
			if (m_currentType == null)
				columnName = getColumnName(rowTime, time, isInteger);
			else
				columnName = getColumnName(rowTime, time);

			m_dataPointWriteBuffer.deleteColumn(m_currentRow, columnName, m_now);
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
			if (m_currentType == null)
				columnName = getColumnName(rowTime, time, datapoint.isLong());
			else
				columnName = getColumnName(rowTime, time);

			m_dataPointWriteBuffer.deleteColumn(m_currentRow, columnName, m_now);
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
