// KairosDB2
// Copyright (C) 2013 Proofpoint, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>
package org.kairosdb.datastore.cassandra;

import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.SliceQuery;
import org.kairosdb.core.datastore.CachedSearchResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.kairosdb.datastore.cassandra.CassandraDatastore.ROW_WIDTH;
import static org.kairosdb.datastore.cassandra.CassandraDatastore.getColumnName;
import static org.kairosdb.datastore.cassandra.CassandraDatastore.getColumnTimestamp;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 1/25/13
 Time: 10:45 PM
 To change this template use File | Settings | File Templates.
 */
public class QueryRunner
{
	public static final DataPointsRowKeySerializer ROW_KEY_SERIALIZER = new DataPointsRowKeySerializer();
	public static final LongOrDoubleSerializer LONG_OR_DOUBLE_SERIALIZER = new LongOrDoubleSerializer();

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
			m_startTime = getColumnName(m_tierRowTime, startTime);

		if (endTime > (m_tierRowTime + ROW_WIDTH))
			m_endTime = ROW_WIDTH;
		else
			m_endTime = getColumnName(m_tierRowTime, endTime);

		m_cachedResults = csResult;
		m_singleRowReadSize = singleRowReadSize;
		m_multiRowReadSize = multiRowReadSize;


	}

	public void runQuery() throws IOException
	{
		MultigetSliceQuery<DataPointsRowKey, Integer, LongOrDouble> msliceQuery =
				HFactory.createMultigetSliceQuery(m_keyspace,
						ROW_KEY_SERIALIZER,
						IntegerSerializer.get(), LONG_OR_DOUBLE_SERIALIZER);

		msliceQuery.setColumnFamily(m_columnFamily);
		msliceQuery.setKeys(m_rowKeys);
		msliceQuery.setRange(m_startTime, m_endTime, false, m_multiRowReadSize);

		Rows<DataPointsRowKey, Integer, LongOrDouble> rows =
				msliceQuery.execute().get();

		List<Row<DataPointsRowKey, Integer, LongOrDouble>> unfinishedRows =
				new ArrayList<Row<DataPointsRowKey, Integer, LongOrDouble>>();

		for (Row<DataPointsRowKey, Integer, LongOrDouble> row : rows)
		{
			List<HColumn<Integer, LongOrDouble>> columns = row.getColumnSlice().getColumns();
			if (columns.size() == m_multiRowReadSize)
				unfinishedRows.add(row);

			writeColumns(row.getKey(), columns);
		}


		//Iterate through the unfinished rows and get the rest of the data.
		//todo: use multiple threads to retrieve this data
		for (Row<DataPointsRowKey, Integer, LongOrDouble> unfinishedRow : unfinishedRows)
		{
			DataPointsRowKey key = unfinishedRow.getKey();

			SliceQuery<DataPointsRowKey, Integer, LongOrDouble> sliceQuery =
					HFactory.createSliceQuery(m_keyspace, ROW_KEY_SERIALIZER,
					IntegerSerializer.get(), LONG_OR_DOUBLE_SERIALIZER);

			sliceQuery.setColumnFamily(m_columnFamily);
			sliceQuery.setKey(key);

			List<HColumn<Integer, LongOrDouble>> columns = unfinishedRow.getColumnSlice().getColumns();

			do
			{
				Integer lastTime = columns.get(columns.size() -1).getName();

				sliceQuery.setRange(lastTime+1, m_endTime, false, m_singleRowReadSize);

				columns = sliceQuery.execute().get().getColumns();
				writeColumns(key, columns);

			} while (columns.size() == m_singleRowReadSize);
		}

		m_cachedResults.endDataPoints();
	}


	private void writeColumns(DataPointsRowKey rowKey, List<HColumn<Integer, LongOrDouble>> columns)
			throws IOException
	{
		Map<String, String> tags = rowKey.getTags();
		m_cachedResults.startDataPointSet(tags);
		//long lastTime = 0;

		for (HColumn<Integer, LongOrDouble> column : columns)
		{
			/*if (column.getName() < lastTime)
				System.out.println("ERROR "+column.getName()+" : "+column.getValue().getLongValue());

			lastTime = column.getName();*/
			LongOrDouble value = column.getValue();
			if (value.isLong())
			{
				m_cachedResults.addDataPoint(getColumnTimestamp(rowKey.getTimestamp(),
						column.getName()), value.getLongValue());
			}
			else
			{
				m_cachedResults.addDataPoint(getColumnTimestamp(rowKey.getTimestamp(),
						column.getName()), value.getDoubleValue());
			}
		}
	}

}
