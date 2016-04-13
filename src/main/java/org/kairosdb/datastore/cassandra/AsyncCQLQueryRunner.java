package org.kairosdb.datastore.cassandra;


import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang3.tuple.Pair;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.datapoints.DataPointFactory;
import org.kairosdb.core.datapoints.LegacyDataPointFactory;
import org.kairosdb.core.datapoints.LegacyDoubleDataPoint;
import org.kairosdb.core.datapoints.LegacyLongDataPoint;
import org.kairosdb.core.datastore.QueryCallback;
import org.kairosdb.util.KDataInput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.datastax.driver.core.DataType.cint;
import static com.datastax.driver.core.ProtocolVersion.NEWEST_SUPPORTED;
import static com.google.common.util.concurrent.Futures.successfulAsList;
import static com.google.common.util.concurrent.Futures.transform;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;
import static org.apache.commons.lang3.tuple.Pair.of;
import static org.kairosdb.datastore.cassandra.CassandraDatastore.getColumnTimestamp;
import static org.kairosdb.datastore.cassandra.CassandraDatastore.isLongValue;

public class AsyncCQLQueryRunner {


    public static final String RANGE_KEY = "column1";
    private static final String VALUE_COLUMN = "value";

    private final Session session;
    private static ExecutorService executor = Executors.newFixedThreadPool(10);
    private final PreparedStatement queryStatement;
    private final KairosDataPointFactory m_kairosDataPointFactory;


    public AsyncCQLQueryRunner(Session session, KairosDataPointFactory kairosDataPointFactory) {
        this.session = session;
        serializer = new DataPointsRowKeySerializer();
        m_kairosDataPointFactory = kairosDataPointFactory;
        queryStatement = session.prepare("SELECT column1, value FROM data_points WHERE key =? AND column1 > ? and column1 < ? ");
    }

    DataPointsRowKeySerializer serializer;

    public List<ListenableFuture<Pair<DataPointsRowKey, List<Pair<Integer, ByteBuffer>>>>> queryForDataPoints(CQLQueryRunnerParams queryRunnerParams) throws ExecutionException, InterruptedException {

        List<ListenableFuture<Pair<DataPointsRowKey, List<Pair<Integer, ByteBuffer>>>>> asyncResults = queryRunnerParams.getRowKeys().stream()
                .map(dataPointsRowKey -> getSingleQueryFuture(dataPointsRowKey, queryRunnerParams.getStartTime(), queryRunnerParams.getEndTime(), queryRunnerParams.isDescending()))
                .collect(toList());

        return asyncResults;

    }

    private ListenableFuture<Pair<DataPointsRowKey, List<Pair<Integer, ByteBuffer>>>> getSingleQueryFuture(DataPointsRowKey row, int startTime, int endTime, boolean descending) {
        ListenableFuture<List<Pair<Integer, ByteBuffer>>> future = sliceQuery(row, startTime, endTime, descending);
        return transform(future, (Function<List<Pair<Integer, ByteBuffer>>, Pair<DataPointsRowKey, List<Pair<Integer, ByteBuffer>>>>) input -> of(row, input));
    }

    private ListenableFuture<List<Pair<Integer, ByteBuffer>>> sliceQuery(DataPointsRowKey row, int startTime, int endTime, boolean descending) {
        ByteBuffer startRange = cint().serialize(startTime, NEWEST_SUPPORTED);
        ByteBuffer endRange = cint().serialize(endTime, NEWEST_SUPPORTED);


        ListenableFuture<List<Pair<Integer, ByteBuffer>>> mappedResult = transform(session.executeAsync(queryStatement.bind(serializer.toByteBuffer(row), startRange, endRange)),
                (ResultSet rows) ->
                        stream(rows.all().spliterator(), false)
                                .map(this::rowToValueData)
                                .collect(toList()), executor);

        return mappedResult;
    }

    private Pair<Integer, ByteBuffer> rowToValueData(Row r) {
        return of((Integer) cint().deserialize(r.getBytes(RANGE_KEY), NEWEST_SUPPORTED), r.getBytes(VALUE_COLUMN));
    }

    public void runCQLQuery(CQLQueryRunnerParams params, QueryCallback callback) {
        try {
            List<ListenableFuture<Pair<DataPointsRowKey, List<Pair<Integer, ByteBuffer>>>>> pairs = this.queryForDataPoints(params);

            ListenableFuture<List<Pair<DataPointsRowKey, List<Pair<Integer, ByteBuffer>>>>> futures = successfulAsList(pairs);

            pairs.stream().forEach(future -> transform(future, new Function<Pair<DataPointsRowKey, List<Pair<Integer, ByteBuffer>>>, Object>() {

                @Nullable
                @Override
                public Object apply(Pair<DataPointsRowKey, List<Pair<Integer, ByteBuffer>>> resultPair) {
                    try {
                        writeColumnsCQL(resultPair.getKey(), resultPair.getValue(), callback);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return resultPair;
                }
            }));

            futures.get();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private synchronized void writeColumnsCQL(DataPointsRowKey rowKey, List<Pair<Integer, ByteBuffer>> columns, QueryCallback queryCallback)
            throws IOException {
        if (columns.size() != 0) {
            Map<String, String> tags = rowKey.getTags();
            String type = rowKey.getDataType();

            queryCallback.startDataPointSet(type, tags);

            DataPointFactory dataPointFactory = m_kairosDataPointFactory.getFactoryForDataStoreType(type);

            for (Pair<Integer, ByteBuffer> column : columns) {
                int columnTime = column.getLeft();

                ByteBuffer value = column.getValue();
                long timestamp = getColumnTimestamp(rowKey.getTimestamp(), columnTime);

                if (type == LegacyDataPointFactory.DATASTORE_TYPE) {
                    if (isLongValue(columnTime)) {
                        queryCallback.addDataPoint(
                                new LegacyLongDataPoint(timestamp,
                                        ValueSerializer.getLongFromByteBuffer(value)));
                    } else {
                        queryCallback.addDataPoint(
                                new LegacyDoubleDataPoint(timestamp,
                                        ValueSerializer.getDoubleFromByteBuffer(value)));
                    }
                } else {
                    queryCallback.addDataPoint(
                            dataPointFactory.getDataPoint(timestamp, KDataInput.createInput(value.array())));
                }
            }
        }
    }

}