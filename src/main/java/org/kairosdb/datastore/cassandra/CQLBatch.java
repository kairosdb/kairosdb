package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.google.common.eventbus.EventBus;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.queue.EventCompletionCallBack;
import org.kairosdb.events.DataPointEvent;
import org.kairosdb.util.KDataOutput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;

import static org.kairosdb.datastore.cassandra.CassandraDatastore.*;
import static org.kairosdb.datastore.cassandra.CassandraDatastore.DATA_POINTS_ROW_KEY_SERIALIZER;

/**
 Created by bhawkins on 1/11/17.
 */
public class CQLBatch
{
	private static final Charset UTF_8 = Charset.forName("UTF-8");

	private final Session m_session;
	private final CassandraDatastore.PreparedStatements m_preparedStatements;
	private final BatchStats m_batchStats;
	private final ConsistencyLevel m_consistencyLevel;
	private final long m_now;
	private final LoadBalancingPolicy m_loadBalancingPolicy;

	//private Map<Host, BatchStatement> m_batchMap = new HashMap<>();

	private BatchStatement metricNamesBatch = new BatchStatement(BatchStatement.Type.UNLOGGED);
	private BatchStatement dataPointBatch = new BatchStatement(BatchStatement.Type.UNLOGGED);
	private BatchStatement rowKeyBatch = new BatchStatement(BatchStatement.Type.UNLOGGED);

	public CQLBatch(
			ConsistencyLevel consistencyLevel, Session session,
			CassandraDatastore.PreparedStatements preparedStatements, BatchStats batchStats,
			LoadBalancingPolicy loadBalancingPolicy)
	{
		m_consistencyLevel = consistencyLevel;
		m_session = session;
		m_preparedStatements = preparedStatements;
		m_batchStats = batchStats;
		m_now = System.currentTimeMillis();
		m_loadBalancingPolicy = loadBalancingPolicy;
	}

	public void addRowKey(String metricName, DataPointsRowKey rowKey, int rowKeyTtl)
	{
		ByteBuffer bb = ByteBuffer.allocate(8);
		bb.putLong(0, rowKey.getTimestamp());

		BoundStatement bs = m_preparedStatements.psRowKeyTimeInsert.bind()
				.setString(0, metricName)
				.setTimestamp(1, new Date(rowKey.getTimestamp()))
				//.setBytesUnsafe(1, bb) //Setting timestamp in a more optimal way
				.setInt(2, rowKeyTtl)
				.setLong(3, m_now);

		bs.setConsistencyLevel(m_consistencyLevel);

		rowKeyBatch.add(bs);

		bs = m_preparedStatements.psRowKeyInsert.bind()
				.setString(0, metricName)
				.setTimestamp(1, new Date(rowKey.getTimestamp()))
				//.setBytesUnsafe(1, bb)  //Setting timestamp in a more optimal way
				.setString(2, rowKey.getDataType())
				.setMap(3, rowKey.getTags())
				.setInt(4, rowKeyTtl);
				//.setLong(5, m_now);

		bs.setConsistencyLevel(m_consistencyLevel);

		rowKeyBatch.add(bs);
	}

	public void addMetricName(String metricName)
	{
		BoundStatement bs = new BoundStatement(m_preparedStatements.psStringIndexInsert);
		bs.setBytesUnsafe(0, ByteBuffer.wrap(ROW_KEY_METRIC_NAMES.getBytes(UTF_8)));
		bs.setBytesUnsafe(1, ByteBuffer.wrap(metricName.getBytes(UTF_8)));
		bs.setConsistencyLevel(m_consistencyLevel);
		metricNamesBatch.add(bs);
	}

	public void addDataPoint(DataPointsRowKey rowKey, int columnTime, DataPoint dataPoint, int ttl) throws IOException
	{
		KDataOutput kDataOutput = new KDataOutput();
		dataPoint.writeValueToBuffer(kDataOutput);

		BoundStatement boundStatement = new BoundStatement(m_preparedStatements.psDataPointsInsert);
		boundStatement.setBytesUnsafe(0, DATA_POINTS_ROW_KEY_SERIALIZER.toByteBuffer(rowKey));
		ByteBuffer b = ByteBuffer.allocate(4);
		b.putInt(columnTime);
		b.rewind();
		boundStatement.setBytesUnsafe(1, b);
		boundStatement.setBytesUnsafe(2, ByteBuffer.wrap(kDataOutput.getBytes()));
		boundStatement.setInt(3, ttl);
		boundStatement.setLong(4, m_now);
		boundStatement.setConsistencyLevel(m_consistencyLevel);
		boundStatement.setIdempotent(true);

		/*Iterator<Host> hosts = m_loadBalancingPolicy.newQueryPlan("kairosdb", boundStatement);
		if (hosts.hasNext())
		{
			Host host = hosts.next();
			BatchStatement batchStatement = m_batchMap.get(host);
			if (batchStatement == null)
			{
				batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
				m_batchMap.put(host, batchStatement);
			}
			batchStatement.add(boundStatement);

		}*/
		dataPointBatch.add(boundStatement);
	}

	public void submitBatch()
	{
		if (metricNamesBatch.size() != 0)
		{
			m_session.executeAsync(metricNamesBatch);
			m_batchStats.addNameBatch(metricNamesBatch.size());
		}

		if (rowKeyBatch.size() != 0)
		{
			//rowKeyBatch.enableTracing();
			ResultSet resultSet = m_session.execute(rowKeyBatch);
			m_batchStats.addRowKeyBatch(rowKeyBatch.size());
		}

		/*for (BatchStatement batchStatement : m_batchMap.values())
		{
			m_session.executeAsync(batchStatement);
			m_batchStats.addDatapointsBatch(batchStatement.size());
		}*/
		if (dataPointBatch.size() != 0)
		{
			m_session.execute(dataPointBatch);
			m_batchStats.addDatapointsBatch(dataPointBatch.size());
		}
	}
}
