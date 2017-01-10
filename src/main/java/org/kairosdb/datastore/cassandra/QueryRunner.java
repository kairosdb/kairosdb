/*
 * Copyright 2016 KairosDB Authors
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
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.SliceQuery;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.datapoints.*;
import org.kairosdb.core.datastore.CachedSearchResult;
import org.kairosdb.core.datastore.Order;
import org.kairosdb.core.datastore.QueryCallback;
import org.kairosdb.util.KDataInput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.kairosdb.datastore.cassandra.CassandraDatastore.*;

public class QueryRunner
{
	public static final DataPointsRowKeySerializer ROW_KEY_SERIALIZER = new DataPointsRowKeySerializer();

	private Keyspace m_keyspace;
	private String m_columnFamily;
	private List<DataPointsRowKey> m_rowKeys;
	private int m_startTime; //relative row time
	private int m_endTime; //relative row time
	private QueryCallback m_queryCallback;
	private int m_singleRowReadSize;
	private int m_multiRowReadSize;
	private boolean m_limit = false;
	private boolean m_descending = false;
	private LongDataPointFactory m_longDataPointFactory = new LongDataPointFactoryImpl();
	private DoubleDataPointFactory m_doubleDataPointFactory = new DoubleDataPointFactoryImpl();

	private final KairosDataPointFactory m_kairosDataPointFactory;

	public QueryRunner(Keyspace keyspace, String columnFamily,
			KairosDataPointFactory kairosDataPointFactory,
			List<DataPointsRowKey> rowKeys, long startTime, long endTime,
			QueryCallback csResult,
			int singleRowReadSize, int multiRowReadSize, int limit, Order order)
	{
		m_keyspace = keyspace;
		m_columnFamily = columnFamily;
		m_rowKeys = rowKeys;
		m_kairosDataPointFactory = kairosDataPointFactory;
		long m_tierRowTime = rowKeys.get(0).getTimestamp();
		if (startTime < m_tierRowTime)
			m_startTime = 0;
		else
			m_startTime = getColumnName(m_tierRowTime, startTime);

		if (endTime > (m_tierRowTime + ROW_WIDTH))
			m_endTime = getColumnName(m_tierRowTime, m_tierRowTime + ROW_WIDTH) +1;
		else
			m_endTime = getColumnName(m_tierRowTime, endTime) +1; //add 1 so we get 0x1 for last bit

		m_queryCallback = csResult;
		m_singleRowReadSize = singleRowReadSize;
		m_multiRowReadSize = multiRowReadSize;

		if (limit != 0)
		{
			m_limit = true;
			m_singleRowReadSize = limit;
			m_multiRowReadSize = limit;
		}

		if (order == Order.DESC)
			m_descending = true;
	}

	public void runQuery() throws IOException
	{
		MultigetSliceQuery<DataPointsRowKey, Integer, byte[]> msliceQuery =
				HFactory.createMultigetSliceQuery(m_keyspace,
						ROW_KEY_SERIALIZER,
						IntegerSerializer.get(), BytesArraySerializer.get());

		msliceQuery.setColumnFamily(m_columnFamily);
		msliceQuery.setKeys(m_rowKeys);
		if (m_descending)
			msliceQuery.setRange(m_endTime, m_startTime, true, m_multiRowReadSize);
		else
			msliceQuery.setRange(m_startTime, m_endTime, false, m_multiRowReadSize);

		Rows<DataPointsRowKey, Integer, byte[]> rows =
				msliceQuery.execute().get();

		List<Row<DataPointsRowKey, Integer, byte[]>> unfinishedRows =
				new ArrayList<Row<DataPointsRowKey, Integer, byte[]>>();

		for (Row<DataPointsRowKey, Integer, byte[]> row : rows)
		{
			List<HColumn<Integer, byte[]>> columns = row.getColumnSlice().getColumns();
			if (!m_limit && columns.size() == m_multiRowReadSize)
				unfinishedRows.add(row);

			writeColumns(row.getKey(), columns);
		}


		//Iterate through the unfinished rows and get the rest of the data.
		//todo: use multiple threads to retrieve this data
		for (Row<DataPointsRowKey, Integer, byte[]> unfinishedRow : unfinishedRows)
		{
			DataPointsRowKey key = unfinishedRow.getKey();

			SliceQuery<DataPointsRowKey, Integer, byte[]> sliceQuery =
					HFactory.createSliceQuery(m_keyspace, ROW_KEY_SERIALIZER,
					IntegerSerializer.get(), BytesArraySerializer.get());

			sliceQuery.setColumnFamily(m_columnFamily);
			sliceQuery.setKey(key);

			List<HColumn<Integer, byte[]>> columns = unfinishedRow.getColumnSlice().getColumns();

			do
			{
				Integer lastTime = columns.get(columns.size() -1).getName();

				if (m_descending)
					sliceQuery.setRange(lastTime-1, m_startTime, true, m_singleRowReadSize);
				else
					sliceQuery.setRange(lastTime+1, m_endTime, false, m_singleRowReadSize);

				columns = sliceQuery.execute().get().getColumns();
				writeColumns(key, columns);
			} while (columns.size() == m_singleRowReadSize);
		}
	}


	private void writeColumns(DataPointsRowKey rowKey, List<HColumn<Integer, byte[]>> columns)
			throws IOException
	{
		if (columns.size() != 0)
		{
			Map<String, String> tags = rowKey.getTags();
			String type = rowKey.getDataType();

			m_queryCallback.startDataPointSet(type, tags);

			DataPointFactory dataPointFactory = null;
			dataPointFactory = m_kairosDataPointFactory.getFactoryForDataStoreType(type);

			for (HColumn<Integer, byte[]> column : columns)
			{
				int columnTime = column.getName();

				byte[] value = column.getValue();
				long timestamp = getColumnTimestamp(rowKey.getTimestamp(), columnTime);

				//If type is legacy type it will point to the same object, no need for equals
				if (type == LegacyDataPointFactory.DATASTORE_TYPE)
				{
					if (isLongValue(columnTime))
					{
						m_queryCallback.addDataPoint(
								new LegacyLongDataPoint(timestamp,
										ValueSerializer.getLongFromByteBuffer(ByteBuffer.wrap(value))));
					}
					else
					{
						m_queryCallback.addDataPoint(
								new LegacyDoubleDataPoint(timestamp,
										ValueSerializer.getDoubleFromByteBuffer(ByteBuffer.wrap(value))));
					}
				}
				else
				{
					m_queryCallback.addDataPoint(
							dataPointFactory.getDataPoint(timestamp, KDataInput.createInput(value)));
				}
			}
		}
	}

}
