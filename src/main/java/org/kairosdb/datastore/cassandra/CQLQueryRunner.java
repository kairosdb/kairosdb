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

import com.datastax.driver.core.Row;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.datastax.driver.core.DataType.cint;
import static com.datastax.driver.core.ProtocolVersion.NEWEST_SUPPORTED;

import static org.kairosdb.datastore.cassandra.CassandraDatastore.*;

public class CQLQueryRunner
{
	public static final DataPointsRowKeySerializer ROW_KEY_SERIALIZER = new DataPointsRowKeySerializer();

	private final Session m_session;
	private final PreparedStatement m_dataPointQuery;

	private List<DataPointsRowKey> m_rowKeys;
	private int m_startTime; // relative row time
	private int m_endTime; // relative row time
	private QueryCallback m_queryCallback;
	private int m_singleRowReadSize;
	private int m_multiRowReadSize;
	private boolean m_limit = false;
	private boolean m_descending = false;
	private LongDataPointFactory m_longDataPointFactory = new LongDataPointFactoryImpl();
	private DoubleDataPointFactory m_doubleDataPointFactory = new DoubleDataPointFactoryImpl();

	private final KairosDataPointFactory m_kairosDataPointFactory;

	public CQLQueryRunner(Session session, PreparedStatement dataPointQuery,
			KairosDataPointFactory kairosDataPointFactory,
			List<DataPointsRowKey> rowKeys, long startTime, long endTime,
			QueryCallback csResult,
			int singleRowReadSize, int multiRowReadSize, int limit, Order order)
	{
		m_session = session;
		m_dataPointQuery = dataPointQuery;

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
			m_endTime = getColumnName(m_tierRowTime, endTime) + 1; //add 1 so we get 0x1 for last bit

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
		BoundStatement query = m_dataPointQuery.bind();

		ByteBuffer startRange = cint().serialize(m_startTime, NEWEST_SUPPORTED);
		ByteBuffer endRange = cint().serialize(m_endTime, NEWEST_SUPPORTED);

		query.setBytes(1, startRange);
		query.setBytes(2, endRange);

		for( DataPointsRowKey k : m_rowKeys) {
			query.setBytes(0, ROW_KEY_SERIALIZER.toByteBuffer(k));

			ResultSet rs = m_session.execute(query);

			if(rs.isExhausted()) {
				continue;
			}

			Map<String, String> tags = k.getTags();
			String type = k.getDataType();

			DataPointFactory dataPointFactory = null;
			dataPointFactory = m_kairosDataPointFactory.getFactoryForDataStoreType(type);

			m_queryCallback.startDataPointSet(type, tags);

			for (Row r : rs) {
				int columnTime = (Integer) cint().deserialize(r.getBytes("column1"), NEWEST_SUPPORTED);
				ByteBuffer value = r.getBytes("value");

				long timestamp = getColumnTimestamp(k.getTimestamp(), columnTime);

				if (type == LegacyDataPointFactory.DATASTORE_TYPE) {
					if (isLongValue(columnTime)) {
						m_queryCallback.addDataPoint(
								new LegacyLongDataPoint(timestamp,
										ValueSerializer.getLongFromByteBuffer(value)));
					} else {
						m_queryCallback.addDataPoint(
								new LegacyDoubleDataPoint(timestamp,
										ValueSerializer.getDoubleFromByteBuffer(value)));
					}
				} else {
					m_queryCallback.addDataPoint(
							dataPointFactory.getDataPoint(timestamp, KDataInput.createInput(value.array())));
				}
			}
		}
	}
}
