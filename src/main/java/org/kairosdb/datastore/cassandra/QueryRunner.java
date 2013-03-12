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

import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.SliceQuery;
import org.kairosdb.core.datastore.CachedSearchResult;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.kairosdb.datastore.cassandra.CassandraDatastore.*;

public class QueryRunner
{
	public static final DataPointsRowKeySerializer ROW_KEY_SERIALIZER = new DataPointsRowKeySerializer();
	public static final ValueSerializer LONG_OR_DOUBLE_SERIALIZER = new ValueSerializer();

	private Keyspace m_keyspace;
	private String m_columnFamily;
	private List<DataPointsRowKey> m_rowKeys;
	private long m_tierRowTime; //Start of the row time for this tier
	private int m_startTime; //relative row time
	private int m_endTime; //relative row time
	private CachedSearchResult m_cachedResults;
	private int m_singleRowReadSize;
	private int m_multiRowReadSize;

	public QueryRunner(Keyspace keyspace, String columnFamily,
			List<DataPointsRowKey> rowKeys, long startTime, long endTime,
			CachedSearchResult csResult,
			int singleRowReadSize, int multiRowReadSize)
	{
		m_keyspace = keyspace;
		m_columnFamily = columnFamily;
		m_rowKeys = rowKeys;
		m_tierRowTime = rowKeys.get(0).getTimestamp(); //Todo pass this value in??
		if (startTime < m_tierRowTime)
			m_startTime = 0;
		else
			m_startTime = getColumnName(m_tierRowTime, startTime, true); //Pass true so we get 0x0 for last bit

		if (endTime > (m_tierRowTime + ROW_WIDTH))
			m_endTime = getColumnName(m_tierRowTime, m_tierRowTime + ROW_WIDTH, false);
		else
			m_endTime = getColumnName(m_tierRowTime, endTime, false); //Pass false so we get 0x1 for last bit

		m_cachedResults = csResult;
		m_singleRowReadSize = singleRowReadSize;
		m_multiRowReadSize = multiRowReadSize;


	}

	public void runQuery() throws IOException
	{
		MultigetSliceQuery<DataPointsRowKey, Integer, ByteBuffer> msliceQuery =
				HFactory.createMultigetSliceQuery(m_keyspace,
						ROW_KEY_SERIALIZER,
						IntegerSerializer.get(), ByteBufferSerializer.get());

		msliceQuery.setColumnFamily(m_columnFamily);
		msliceQuery.setKeys(m_rowKeys);
		msliceQuery.setRange(m_startTime, m_endTime, false, m_multiRowReadSize);

		Rows<DataPointsRowKey, Integer, ByteBuffer> rows =
				msliceQuery.execute().get();

		List<Row<DataPointsRowKey, Integer, ByteBuffer>> unfinishedRows =
				new ArrayList<Row<DataPointsRowKey, Integer, ByteBuffer>>();

		for (Row<DataPointsRowKey, Integer, ByteBuffer> row : rows)
		{
			List<HColumn<Integer, ByteBuffer>> columns = row.getColumnSlice().getColumns();
			if (columns.size() == m_multiRowReadSize)
				unfinishedRows.add(row);

			writeColumns(row.getKey(), columns);
		}


		//Iterate through the unfinished rows and get the rest of the data.
		//todo: use multiple threads to retrieve this data
		for (Row<DataPointsRowKey, Integer, ByteBuffer> unfinishedRow : unfinishedRows)
		{
			DataPointsRowKey key = unfinishedRow.getKey();

			SliceQuery<DataPointsRowKey, Integer, ByteBuffer> sliceQuery =
					HFactory.createSliceQuery(m_keyspace, ROW_KEY_SERIALIZER,
					IntegerSerializer.get(), ByteBufferSerializer.get());

			sliceQuery.setColumnFamily(m_columnFamily);
			sliceQuery.setKey(key);

			List<HColumn<Integer, ByteBuffer>> columns = unfinishedRow.getColumnSlice().getColumns();

			do
			{
				Integer lastTime = columns.get(columns.size() -1).getName();

				sliceQuery.setRange(lastTime+1, m_endTime, false, m_singleRowReadSize);

				columns = sliceQuery.execute().get().getColumns();
				writeColumns(key, columns);

			} while (columns.size() == m_singleRowReadSize);
		}

		//m_cachedResults.endDataPoints();
	}


	private void writeColumns(DataPointsRowKey rowKey, List<HColumn<Integer, ByteBuffer>> columns)
			throws IOException
	{
		Map<String, String> tags = rowKey.getTags();
		m_cachedResults.startDataPointSet(tags);

		for (HColumn<Integer, ByteBuffer> column : columns)
		{
			int columnTime = column.getName();

			ByteBuffer value = column.getValue();
			if (isLongValue(columnTime))
			{
				m_cachedResults.addDataPoint(getColumnTimestamp(rowKey.getTimestamp(),
						columnTime), ValueSerializer.getLongFromByteBuffer(value));
			}
			else
			{
				m_cachedResults.addDataPoint(getColumnTimestamp(rowKey.getTimestamp(),
						columnTime), ValueSerializer.getDoubleFromByteBuffer(value));
			}
		}
	}

}
