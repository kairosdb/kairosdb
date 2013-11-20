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
import org.kairosdb.core.datastore.Order;
import org.kairosdb.core.datastore.QueryCallback;
import org.kairosdb.util.MemoryMonitor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import static org.kairosdb.datastore.cassandra.CassandraDatastore.*;

public class QueryRunner
{
	public static final DataPointsRowKeySerializer ROW_KEY_SERIALIZER = new DataPointsRowKeySerializer();

	private Keyspace m_keyspace;
	private String m_columnFamily;
	private long m_queryStartTime;
	private long m_queryEndTime;
	private boolean m_descending = false;
	private QueryCallback m_queryCallback;
	private Object m_callbackLock = new Object();
	private int m_singleRowReadSize;
	private int m_multiRowReadSize;
	private boolean m_limit = false;
	private List<Runner> m_runners = new ArrayList<Runner>();
	private MemoryMonitor m_memoryMonitor = new MemoryMonitor(1);
	private ExecutorService m_threadPool;


	public QueryRunner(Keyspace keyspace, String columnFamily,
			long startTime, long endTime, QueryCallback csResult,
			int singleRowReadSize, int multiRowReadSize, int limit, Order order,
			ExecutorService threadPool)
	{
		m_keyspace = keyspace;
		m_columnFamily = columnFamily;

		m_queryStartTime = startTime;
		m_queryEndTime = endTime;

		m_queryCallback = csResult;
		m_singleRowReadSize = singleRowReadSize;
		m_multiRowReadSize = multiRowReadSize;

		m_threadPool = threadPool;

		if (limit != 0)
		{
			m_limit = true;
			m_singleRowReadSize = limit;
			m_multiRowReadSize = limit;
		}

		if (order == Order.DESC)
			m_descending = true;
	}

	public void addRunner(List<DataPointsRowKey> rowKeys)
	{
		Runner runner = new Runner(rowKeys);
		m_runners.add(runner);
	}


	public void runQuery() throws IOException
	{
		try
		{
			System.out.println(m_runners.size() +" Runners");
			m_threadPool.invokeAll(m_runners);
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
		}
	}


	private void writeColumns(DataPointsRowKey rowKey, List<HColumn<Integer, ByteBuffer>> columns)
			throws IOException
	{
		if (columns.size() != 0)
		{
			Map<String, String> tags = rowKey.getTags();

			synchronized (m_callbackLock)
			{
				m_queryCallback.startDataPointSet(tags);

				for (HColumn<Integer, ByteBuffer> column : columns)
				{
					int columnTime = column.getName();

					ByteBuffer value = column.getValue();
					if (isLongValue(columnTime))
					{
						m_queryCallback.addDataPoint(getColumnTimestamp(rowKey.getTimestamp(),
								columnTime), ValueSerializer.getLongFromByteBuffer(value));
					}
					else
					{
						m_queryCallback.addDataPoint(getColumnTimestamp(rowKey.getTimestamp(),
								columnTime), ValueSerializer.getDoubleFromByteBuffer(value));
					}
				}
			}

			m_memoryMonitor.checkMemoryAndThrowException();
		}
	}

	private class Runner implements Callable<String>
	{
		private int m_startTime; //relative row time
		private int m_endTime; //relative row time
		private List<DataPointsRowKey> m_rowKeys;

		public Runner(List<DataPointsRowKey> rowKeys)
		{
			m_rowKeys = rowKeys;

			long tierRowTime = rowKeys.get(0).getTimestamp();
			if (m_queryStartTime < tierRowTime)
				m_startTime = 0;
			else
				m_startTime = getColumnName(tierRowTime, m_queryStartTime, true); //Pass true so we get 0x0 for last bit

			if (m_queryEndTime > (tierRowTime + ROW_WIDTH))
				m_endTime = getColumnName(tierRowTime, tierRowTime + ROW_WIDTH, false);
			else
				m_endTime = getColumnName(tierRowTime, m_queryEndTime, false); //Pass false so we get 0x1 for last bit
		}


		@Override
		public String call() throws Exception
		{
			MultigetSliceQuery<DataPointsRowKey, Integer, ByteBuffer> msliceQuery =
					HFactory.createMultigetSliceQuery(m_keyspace,
							ROW_KEY_SERIALIZER,
							IntegerSerializer.get(), ByteBufferSerializer.get());

			msliceQuery.setColumnFamily(m_columnFamily);
			msliceQuery.setKeys(m_rowKeys);
			if (m_descending)
				msliceQuery.setRange(m_endTime, m_startTime, true, m_multiRowReadSize);
			else
				msliceQuery.setRange(m_startTime, m_endTime, false, m_multiRowReadSize);

			Rows<DataPointsRowKey, Integer, ByteBuffer> rows =
					msliceQuery.execute().get();

			List<Row<DataPointsRowKey, Integer, ByteBuffer>> unfinishedRows =
					new ArrayList<Row<DataPointsRowKey, Integer, ByteBuffer>>();

			for (Row<DataPointsRowKey, Integer, ByteBuffer> row : rows)
			{
				List<HColumn<Integer, ByteBuffer>> columns = row.getColumnSlice().getColumns();
				if (!m_limit && columns.size() == m_multiRowReadSize)
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

					if (m_descending)
						sliceQuery.setRange(lastTime-1, m_startTime, true, m_singleRowReadSize);
					else
						sliceQuery.setRange(lastTime+1, m_endTime, false, m_singleRowReadSize);

					columns = sliceQuery.execute().get().getColumns();
					writeColumns(key, columns);
				} while (columns.size() == m_singleRowReadSize);
			}

			return ("");
		}
	}

}
