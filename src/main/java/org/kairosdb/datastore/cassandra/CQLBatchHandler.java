package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.eventbus.EventBus;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.queue.EventCompletionCallBack;
import org.kairosdb.events.DataPointEvent;
import org.kairosdb.util.KDataOutput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;

import static org.kairosdb.datastore.cassandra.CassandraDatastore.*;
import static org.kairosdb.datastore.cassandra.CassandraDatastore.DATA_POINTS_ROW_KEY_SERIALIZER;

/**
 Created by bhawkins on 1/11/17.
 */
public class CQLBatchHandler extends BatchHandler
{
	private static final Charset UTF_8 = Charset.forName("UTF-8");

	private final Session m_session;
	private final PreparedStatement m_psInsertData;
	private final PreparedStatement m_psInsertRowKey;
	private final PreparedStatement m_psInsertString;

	private BatchStatement metricNamesBatch = new BatchStatement(BatchStatement.Type.UNLOGGED);
	private BatchStatement dataPointBatch = new BatchStatement(BatchStatement.Type.UNLOGGED);
	private BatchStatement rowKeyBatch = new BatchStatement(BatchStatement.Type.UNLOGGED);

	public CQLBatchHandler(List<DataPointEvent> events, EventCompletionCallBack callBack,
			int defaultTtl, DataCache<DataPointsRowKey>
			rowKeyCache, DataCache<String> metricNameCache, EventBus eventBus,
			Session session, PreparedStatement psInsertData,
			PreparedStatement psInsertRowKey, PreparedStatement psInsertString)
	{
		super(events, callBack, defaultTtl, rowKeyCache, metricNameCache, eventBus);


		m_session = session;
		m_psInsertData = psInsertData;
		m_psInsertRowKey = psInsertRowKey;
		m_psInsertString = psInsertString;
	}

	@Override
	public void addRowKey(String metricName, DataPointsRowKey rowKey, int rowKeyTtl)
	{
		BoundStatement bs = new BoundStatement(m_psInsertRowKey);
		bs.setBytesUnsafe(0, ByteBuffer.wrap(metricName.getBytes(UTF_8)));
		bs.setBytesUnsafe(1, DATA_POINTS_ROW_KEY_SERIALIZER.toByteBuffer(rowKey));
		bs.setInt(2, rowKeyTtl);
		rowKeyBatch.add(bs);
	}

	@Override
	public void addMetricName(String metricName)
	{
		BoundStatement bs = new BoundStatement(m_psInsertString);
		bs.setBytesUnsafe(0, ByteBuffer.wrap(ROW_KEY_METRIC_NAMES.getBytes(UTF_8)));
		bs.setBytesUnsafe(1, ByteBuffer.wrap(metricName.getBytes(UTF_8)));

		metricNamesBatch.add(bs);
	}

	@Override
	public void addDataPoint(DataPointsRowKey rowKey, int columnTime, DataPoint dataPoint, int ttl) throws IOException
	{
		KDataOutput kDataOutput = new KDataOutput();
		dataPoint.writeValueToBuffer(kDataOutput);

		BoundStatement boundStatement = new BoundStatement(m_psInsertData);
		boundStatement.setBytesUnsafe(0, DATA_POINTS_ROW_KEY_SERIALIZER.toByteBuffer(rowKey));
		ByteBuffer b = ByteBuffer.allocate(4);
		b.putInt(columnTime);
		b.rewind();
		boundStatement.setBytesUnsafe(1, b);
		boundStatement.setBytesUnsafe(2, ByteBuffer.wrap(kDataOutput.getBytes()));
		boundStatement.setInt(3, ttl);

		dataPointBatch.add(boundStatement);
	}

	@Override
	public void submitBatch()
	{
		if (metricNamesBatch.size() != 0)
			m_session.executeAsync(metricNamesBatch);

		if (rowKeyBatch.size() != 0)
			m_session.executeAsync(rowKeyBatch);

		if (dataPointBatch.size() != 0)
			m_session.execute(dataPointBatch);
	}
}
