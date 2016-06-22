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
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Session;


import static com.datastax.driver.core.DataType.cint;
import static com.datastax.driver.core.ProtocolVersion.NEWEST_SUPPORTED;

import static org.kairosdb.datastore.cassandra.CassandraDatastore.*;

public class CQLQueryRunner {
    public static final DataPointsRowKeySerializer ROW_KEY_SERIALIZER = new DataPointsRowKeySerializer();

    private final Session m_session;
    private final PreparedStatement m_dataPointQuery;

    private List<DataPointsRowKey> m_rowKeys;
    private long m_startTime; // relative row time
    private long m_endTime; // relative row time
    private long m_rowWidth;
    private QueryCallback m_queryCallback;

    private final KairosDataPointFactory m_kairosDataPointFactory;

    public CQLQueryRunner(Session session, PreparedStatement dataPointQuery,
                          KairosDataPointFactory kairosDataPointFactory,
                          List<DataPointsRowKey> rowKeys, long startTime, long endTime, long rowWidth,
                          QueryCallback csResult,
                          int limit, Order order) {
        m_session = session;
        m_dataPointQuery = dataPointQuery;

        m_rowKeys = rowKeys;
        m_kairosDataPointFactory = kairosDataPointFactory;

        m_startTime = startTime;
        m_endTime = endTime;
        m_rowWidth = rowWidth;

        m_queryCallback = csResult;
    }

    private static class KeyFuturePair {
        DataPointsRowKey key;
        ResultSetFuture future;

        public KeyFuturePair(DataPointsRowKey key, ResultSetFuture future) {
            this.key = key;
            this.future = future;
        }
    }

    private int getStarTime(long startTime, long tierTime) {
        if (startTime < tierTime)
            return 0;
        else
            return getColumnName(tierTime, startTime);
    }

    private int getEndTime(long endTime, long rowTime, long rowWidth) {
        if (endTime > (rowTime + rowWidth))
            return getColumnName(rowTime, rowTime + rowWidth) + 1;
        else
            return getColumnName(rowTime, endTime) + 1; //add 1 so we get 0x1 for last bit
    }

    public void runQuery() throws IOException {
        List<KeyFuturePair> futureResults = new ArrayList<>(m_rowKeys.size());

        for (DataPointsRowKey k : m_rowKeys) {
            BoundStatement query = m_dataPointQuery.bind();
            query.setFetchSize(1000);

            ByteBuffer startRange = cint().serialize(getStarTime(m_startTime, k.getTimestamp()), NEWEST_SUPPORTED);
            ByteBuffer endRange = cint().serialize(getEndTime(m_endTime, k.getTimestamp(), m_rowWidth), NEWEST_SUPPORTED);

            query.setBytes(1, startRange);
            query.setBytes(2, endRange);

            ByteBuffer rowKey = ROW_KEY_SERIALIZER.toByteBuffer(k).duplicate();
            query.setBytes(0, rowKey);

            ResultSetFuture rs = m_session.executeAsync(query);
            futureResults.add(new KeyFuturePair(k, rs));
        }

        for (KeyFuturePair f : futureResults) {
            ResultSet rs = null;
            try {
                rs = f.future.getUninterruptibly();
            } catch (Throwable t) {
                logger.error("Failed to get result", t);
                continue;
            }

            DataPointsRowKey k = f.key;

            Map<String, String> tags = k.getTags();
            String type = k.getDataType();

            DataPointFactory dataPointFactory = m_kairosDataPointFactory.getFactoryForDataStoreType(type);

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
