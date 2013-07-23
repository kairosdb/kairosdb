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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.SetMultimap;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.SliceQuery;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datastore.CachedSearchResult;
import org.kairosdb.core.datastore.DataPointRow;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.datastore.DatastoreMetricQuery;
import org.kairosdb.core.exception.DatastoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkNotNull;

public class CassandraDatastore implements Datastore
{
	public static final Logger logger = LoggerFactory.getLogger(CassandraDatastore.class);

	public static final int ROW_KEY_CACHE_SIZE = 1024;
	public static final int STRING_CACHE_SIZE = 1024;

	public static final int LONG_FLAG = 0x0;
	public static final int FLOAT_FLAG = 0x1;

	public static final DataPointsRowKeySerializer DATA_POINTS_ROW_KEY_SERIALIZER = new DataPointsRowKeySerializer();

	public static final String HOST_LIST_PROPERTY = "kairosdb.datastore.cassandra.host_list";
	public static final String REPLICATION_FACTOR_PROPERTY = "kairosdb.datastore.cassandra.replication_factor";
	public static final long ROW_WIDTH = 1814400000L; //3 Weeks wide
	public static final String WRITE_DELAY_PROPERTY = "kairosdb.datastore.cassandra.write_delay";
	public static final String WRITE_BUFFER_SIZE = "kairosdb.datastore.cassandra.write_buffer_max_size";
	public static final String SINGLE_ROW_READ_SIZE_PROPERTY = "kairosdb.datastore.cassandra.single_row_read_size";
	public static final String MULTI_ROW_READ_SIZE_PROPERTY = "kairosdb.datastore.cassandra.multi_row_read_size";
	public static final String MULTI_ROW_SIZE_PROPERTY = "kairosdb.datastore.cassandra.multi_row_size";

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
	public CassandraDatastore(@Named(HOST_LIST_PROPERTY) String cassandraHostList,
	                          @Named(CassandraModule.CASSANDRA_AUTH_MAP) Map<String, String> cassandraAuthentication,
	                          @Named(REPLICATION_FACTOR_PROPERTY) int replicationFactor,
	                          @Named(SINGLE_ROW_READ_SIZE_PROPERTY) int singleRowReadSize,
	                          @Named(MULTI_ROW_SIZE_PROPERTY) int multiRowSize,
	                          @Named(MULTI_ROW_READ_SIZE_PROPERTY) int multiRowReadSize,
	                          @Named(WRITE_DELAY_PROPERTY) int writeDelay,
	                          @Named(WRITE_BUFFER_SIZE) int maxWriteSize,
	                          final @Named("HOSTNAME") String hostname) throws DatastoreException
	{
		try
		{
			m_singleRowReadSize = singleRowReadSize;
			m_multiRowSize = multiRowSize;
			m_multiRowReadSize = multiRowReadSize;

			CassandraHostConfigurator hostConfig = new CassandraHostConfigurator(cassandraHostList);
			//TODO: fine tune the hostConfig

			m_cluster = HFactory.getOrCreateCluster("kairosdb-cluster",
					hostConfig, cassandraAuthentication);

			KeyspaceDefinition keyspaceDef = m_cluster.describeKeyspace(KEYSPACE);

			if (keyspaceDef == null)
				createSchema(replicationFactor);

			m_keyspace = HFactory.createKeyspace(KEYSPACE, m_cluster);

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
							dps.addDataPoint(new DataPoint(System.currentTimeMillis(), pendingWrites));
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
							dps.addDataPoint(new DataPoint(System.currentTimeMillis(), pendingWrites));
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
							dps.addDataPoint(new DataPoint(System.currentTimeMillis(), pendingWrites));
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
					rowKey = new DataPointsRowKey(dps.getName(), rowTime, dps.getTags());

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

				int columnTime = getColumnName(rowTime, dp.getTimestamp(), dp.isInteger());
				if (dp.isInteger())
				{
					m_dataPointWriteBuffer.addData(rowKey, columnTime,
							ValueSerializer.toByteBuffer(dp.getLongValue()), writeTime);
				}
				else
				{
					m_dataPointWriteBuffer.addData(rowKey, columnTime,
							ValueSerializer.toByteBuffer((float)dp.getDoubleValue()), writeTime);
				}
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
				new ColumnSliceIterator<String, String, String>(sliceQuery, "", (String)null, false, m_singleRowReadSize);

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
				new ColumnSliceIterator<String, String, String>(sliceQuery, "", (String)null, false, m_singleRowReadSize);

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
				new ColumnSliceIterator<String, String, String>(sliceQuery, "", (String)null, false, m_singleRowReadSize);

		List<String> ret = new ArrayList<String>();

		while (columnIterator.hasNext())
			ret.add(columnIterator.next().getName());

		return (ret);
	}

	@Override
	public List<DataPointRow> queryDatabase(DatastoreMetricQuery query, CachedSearchResult cachedSearchResult)
	{
		return queryWithRowKeys(query, cachedSearchResult, getKeysForQuery(query));
	}

	private List<DataPointRow> queryWithRowKeys(DatastoreMetricQuery query,
	                                            CachedSearchResult cachedSearchResult,
	                                            ListMultimap<Long, DataPointsRowKey> rowKeys )
	{
		List<QueryRunner> runners = new ArrayList<QueryRunner>();

		for (Long ts : rowKeys.keySet())
		{
			List<DataPointsRowKey> tierKeys = rowKeys.get(ts);

			for (int keyChunk = 0; keyChunk < tierKeys.size(); keyChunk += m_multiRowSize)
			{
				int chunkEnd = (keyChunk + m_multiRowSize > tierKeys.size() ? tierKeys.size() : keyChunk + m_multiRowSize);

				QueryRunner qRunner = new QueryRunner(m_keyspace, CF_DATA_POINTS, tierKeys.subList(keyChunk, chunkEnd),
						query.getStartTime(), query.getEndTime(), cachedSearchResult, m_singleRowReadSize,
						m_multiRowReadSize);

				runners.add(qRunner);
			}
		}

		try
		{
			//TODO: Run this with multiple threads
			for (QueryRunner runner : runners)
			{
				runner.runQuery();
			}

			cachedSearchResult.endDataPoints();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}

		return cachedSearchResult.getRows();
	}

	@Override
	public void deleteDataPoints(DatastoreMetricQuery deleteQuery, CachedSearchResult cachedSearchResult) throws DatastoreException
	{
		checkNotNull(deleteQuery);
		checkNotNull(cachedSearchResult);

		long now = System.currentTimeMillis();

		ListMultimap<Long, DataPointsRowKey> rowKeys = getKeysForQuery(deleteQuery);

		Iterator<DataPointsRowKey> rowKeyIterator = rowKeys.values().iterator();
		while(rowKeyIterator.hasNext())
		{
			DataPointsRowKey rowKey = rowKeyIterator.next();
			long rowKeyTimestamp = rowKey.getTimestamp();
			if (deleteQuery.getStartTime() <= rowKeyTimestamp && (deleteQuery.getEndTime() >= rowKeyTimestamp + ROW_WIDTH - 1))
			{
				m_dataPointWriteBuffer.deleteRow(rowKey, now);  // delete the whole row
				m_rowKeyWriteBuffer.deleteRow(rowKey.getMetricName(), now); // Delete the index
				rowKeyIterator.remove();
			}
		}

		List<DataPointRow> dataPointRows = queryWithRowKeys(deleteQuery, cachedSearchResult, rowKeys);
		for (DataPointRow row : dataPointRows)
		{
			SortedMap<String, String> tags = getTags(row);
			while(row.hasNext())
			{
				// Delete remaining partial rows
				DataPoint column = row.next();
				long rowTime = calculateRowTime(column.getTimestamp());
				DataPointsRowKey rowKey = new DataPointsRowKey(row.getName(), rowTime, tags);
				int columnName = getColumnName(rowTime, column.getTimestamp(), column.isInteger());
				m_dataPointWriteBuffer.deleteColumn(rowKey, columnName, now);
			}
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
	 Returns the row keys for the query in tiers ie grouped by row key timestamp
	 @param query query
	 @return row keys for the query
	 */
	/*package*/ ListMultimap<Long, DataPointsRowKey> getKeysForQuery(DatastoreMetricQuery query)
	{
		ListMultimap<Long, DataPointsRowKey> retMap = ArrayListMultimap.create();

		SliceQuery<String, DataPointsRowKey, String> sliceQuery =
				HFactory.createSliceQuery(m_keyspace, StringSerializer.get(),
						DATA_POINTS_ROW_KEY_SERIALIZER, StringSerializer.get());

		DataPointsRowKey startKey = new DataPointsRowKey(query.getName(),
				calculateRowTime(query.getStartTime()));

		/*
		Adding 1 to the end time ensures we get all the keys that have end time and
		have tags in the key.
		 */
		DataPointsRowKey endKey = new DataPointsRowKey(query.getName(),
				calculateRowTime(query.getEndTime()) + 1);


		sliceQuery.setColumnFamily(CF_ROW_KEY_INDEX)
				.setKey(query.getName());

		ColumnSliceIterator<String, DataPointsRowKey, String> iterator =
				new ColumnSliceIterator<String, DataPointsRowKey, String>(sliceQuery,
						startKey, endKey, false, m_singleRowReadSize);

		SetMultimap<String, String> filterTags = query.getTags();
		outer: while (iterator.hasNext())
		{
			DataPointsRowKey rowKey = iterator.next().getName();

			Map<String, String> keyTags = rowKey.getTags();
			for (String tag : filterTags.keySet())
			{
				String value = keyTags.get(tag);
				if (value == null || !filterTags.get(tag).contains(value))
					continue outer; //Don't want this key
			}

			retMap.put(rowKey.getTimestamp(), rowKey);
			//ret.add(rowKey);
		}

		if (logger.isDebugEnabled())
			logger.debug("Querying the database using " + retMap.size() + " keys");

		return (retMap);
	}

	public static long calculateRowTime(long timestamp)
	{
		return (timestamp - (timestamp % ROW_WIDTH));
	}


	@SuppressWarnings("PointlessBitwiseExpression")
	public static int getColumnName(long rowTime, long timestamp, boolean isInteger)
	{
		int ret = (int)(timestamp - rowTime);

		if (isInteger)
			return ((ret << 1) | LONG_FLAG);
		else
			return ((ret << 1) | FLOAT_FLAG);

	}

	public static long getColumnTimestamp(long rowTime, int columnName)
	{
		return (rowTime + (long)(columnName >>> 1));
	}

	public static boolean isLongValue(int columnName)
	{
		return ((columnName & 0x1) == LONG_FLAG);
	}
}
