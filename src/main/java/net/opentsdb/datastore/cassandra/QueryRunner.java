// OpenTSDB2
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
package net.opentsdb.datastore.cassandra;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.SliceQuery;
import net.opentsdb.core.datastore.CachedSearchResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
	private long m_startTime;
	private long m_endTime;
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
		m_startTime = startTime;
		m_endTime = endTime;
		m_cachedResults = csResult;
		m_singleRowReadSize = singleRowReadSize;
		m_multiRowReadSize = multiRowReadSize;
	}

	public void runQuery() throws IOException
	{
		MultigetSliceQuery<DataPointsRowKey, Long, LongOrDouble> msliceQuery =
				HFactory.createMultigetSliceQuery(m_keyspace,
						ROW_KEY_SERIALIZER,
						LongSerializer.get(), LONG_OR_DOUBLE_SERIALIZER);

		msliceQuery.setColumnFamily(m_columnFamily);
		msliceQuery.setKeys(m_rowKeys);
		msliceQuery.setRange(m_startTime, m_endTime, false, m_multiRowReadSize);

		Rows<DataPointsRowKey, Long, LongOrDouble> rows =
				msliceQuery.execute().get();

		List<Row<DataPointsRowKey, Long, LongOrDouble>> unfinishedRows =
				new ArrayList<Row<DataPointsRowKey, Long, LongOrDouble>>();

		for (Row<DataPointsRowKey, Long, LongOrDouble> row : rows)
		{
			List<HColumn<Long, LongOrDouble>> columns = row.getColumnSlice().getColumns();
			if (columns.size() == m_multiRowReadSize)
				unfinishedRows.add(row);

			writeColumns(row.getKey().getTags(), columns);
		}


		//Iterate through the unfinished rows and get the rest of the data.
		//todo: use multiple threads to retrieve this data
		for (Row<DataPointsRowKey, Long, LongOrDouble> unfinishedRow : unfinishedRows)
		{
			DataPointsRowKey key = unfinishedRow.getKey();

			SliceQuery<DataPointsRowKey,Long,LongOrDouble> sliceQuery =
					HFactory.createSliceQuery(m_keyspace, ROW_KEY_SERIALIZER,
					LongSerializer.get(), LONG_OR_DOUBLE_SERIALIZER);

			sliceQuery.setColumnFamily(m_columnFamily);
			sliceQuery.setKey(key);

			List<HColumn<Long, LongOrDouble>> columns = unfinishedRow.getColumnSlice().getColumns();

			do
			{
				Long lastTime = columns.get(columns.size() -1).getName();

				sliceQuery.setRange(lastTime+1, m_endTime, false, m_singleRowReadSize);

				columns = sliceQuery.execute().get().getColumns();
				writeColumns(key.getTags(), columns);

			} while (columns.size() == m_singleRowReadSize);
		}

		m_cachedResults.endDataPoints();
	}


	private void writeColumns(Map<String, String> tags, List<HColumn<Long, LongOrDouble>> columns)
			throws IOException
	{
		m_cachedResults.startDataPointSet(tags);
		//long lastTime = 0;

		for (HColumn<Long, LongOrDouble> column : columns)
		{
			/*if (column.getName() < lastTime)
				System.out.println("ERROR "+column.getName()+" : "+column.getValue().getLongValue());

			lastTime = column.getName();*/
			LongOrDouble value = column.getValue();
			if (value.isLong())
				m_cachedResults.addDataPoint(column.getName(), value.getLongValue());
			else
				m_cachedResults.addDataPoint(column.getName(), value.getDoubleValue());
		}
	}

}
