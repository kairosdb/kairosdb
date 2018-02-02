package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import org.kairosdb.core.DataPoint;
import org.kairosdb.util.KDataOutput;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.kairosdb.datastore.cassandra.CassandraConfiguration.KEYSPACE_PROPERTY;
import static org.kairosdb.datastore.cassandra.CassandraDatastore.DATA_POINTS_ROW_KEY_SERIALIZER;
import static org.kairosdb.datastore.cassandra.CassandraDatastore.ROW_KEY_METRIC_NAMES;

/**
 Created by bhawkins on 1/11/17.
 */
public class CQLBatch
{
	private static final Charset UTF_8 = Charset.forName("UTF-8");

	private final Session m_session;
	private final Schema m_schema;
	private final BatchStats m_batchStats;
	private final ConsistencyLevel m_consistencyLevel;
	private final long m_now;
	private final LoadBalancingPolicy m_loadBalancingPolicy;

	private Map<Host, BatchStatement> m_batchMap = new HashMap<>();

	private BatchStatement metricNamesBatch = new BatchStatement(BatchStatement.Type.UNLOGGED);
	private BatchStatement dataPointBatch = new BatchStatement(BatchStatement.Type.UNLOGGED);
	private BatchStatement rowKeyBatch = new BatchStatement(BatchStatement.Type.UNLOGGED);

	@Inject
	@Named(KEYSPACE_PROPERTY)
	private String m_keyspace = "kairosdb";

	@Inject
	public CQLBatch(
			ConsistencyLevel consistencyLevel, Session session,
			Schema schema, BatchStats batchStats,
			LoadBalancingPolicy loadBalancingPolicy)
	{
		m_consistencyLevel = consistencyLevel;
		m_session = session;
		m_schema = schema;
		m_batchStats = batchStats;
		m_now = System.currentTimeMillis();
		m_loadBalancingPolicy = loadBalancingPolicy;
	}

	public void addRowKey(String metricName, DataPointsRowKey rowKey, int rowKeyTtl)
	{
		ByteBuffer bb = ByteBuffer.allocate(8);
		bb.putLong(0, rowKey.getTimestamp());

		Statement bs = m_schema.psRowKeyTimeInsert.bind()
				.setString(0, metricName)
				.setTimestamp(1, new Date(rowKey.getTimestamp()))
				//.setBytesUnsafe(1, bb) //Setting timestamp in a more optimal way
				.setInt(2, rowKeyTtl)
				.setIdempotent(true);

		bs.setConsistencyLevel(m_consistencyLevel);

		rowKeyBatch.add(bs);

		bs = m_schema.psRowKeyInsert.bind()
				.setString(0, metricName)
				.setTimestamp(1, new Date(rowKey.getTimestamp()))
				//.setBytesUnsafe(1, bb)  //Setting timestamp in a more optimal way
				.setString(2, rowKey.getDataType())
				.setMap(3, rowKey.getTags())
				.setInt(4, rowKeyTtl)
				.setIdempotent(true);

		bs.setConsistencyLevel(m_consistencyLevel);

		rowKeyBatch.add(bs);
	}

	public void addMetricName(String metricName)
	{
		BoundStatement bs = new BoundStatement(m_schema.psStringIndexInsert);
		bs.setBytesUnsafe(0, ByteBuffer.wrap(ROW_KEY_METRIC_NAMES.getBytes(UTF_8)));
		bs.setString(1, metricName);
		bs.setConsistencyLevel(m_consistencyLevel);
		metricNamesBatch.add(bs);
	}

	private void addBoundStatement(BoundStatement boundStatement)
	{
		Iterator<Host> hosts = m_loadBalancingPolicy.newQueryPlan(m_keyspace, boundStatement);
		if (hosts.hasNext())
		{
			Host hostKey = hosts.next();

			BatchStatement batchStatement = m_batchMap.get(hostKey);
			if (batchStatement == null)
			{
				batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
				m_batchMap.put(hostKey, batchStatement);
			}
			batchStatement.add(boundStatement);
		}
		else
		{
			dataPointBatch.add(boundStatement);
		}
	}

	public void deleteDataPoint(DataPointsRowKey rowKey, int columnTime) throws IOException
	{
		BoundStatement boundStatement = new BoundStatement(m_schema.psDataPointsDelete);
		boundStatement.setBytesUnsafe(0, DATA_POINTS_ROW_KEY_SERIALIZER.toByteBuffer(rowKey));
		ByteBuffer b = ByteBuffer.allocate(4);
		b.putInt(columnTime);
		b.rewind();
		boundStatement.setBytesUnsafe(1, b);

		boundStatement.setConsistencyLevel(m_consistencyLevel);
		boundStatement.setIdempotent(true);

		addBoundStatement(boundStatement);
	}

	public void addDataPoint(DataPointsRowKey rowKey, int columnTime, DataPoint dataPoint, int ttl) throws IOException
	{
		KDataOutput kDataOutput = new KDataOutput();
		dataPoint.writeValueToBuffer(kDataOutput);

		BoundStatement boundStatement = new BoundStatement(m_schema.psDataPointsInsert);
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

		addBoundStatement(boundStatement);
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
			m_session.executeAsync(rowKeyBatch);
			m_batchStats.addRowKeyBatch(rowKeyBatch.size());
		}

		for (BatchStatement batchStatement : m_batchMap.values())
		{
			//batchStatement.enableTracing();
			if (batchStatement.size() != 0)
			{
				m_session.execute(batchStatement);
				//System.out.println(resultSet.getExecutionInfo().getQueryTrace().getTraceId());
				m_batchStats.addDatapointsBatch(batchStatement.size());
			}
		}

		//Catch all in case of a load balancing problem
		if (dataPointBatch.size() != 0)
		{
			m_session.execute(dataPointBatch);
			m_batchStats.addDatapointsBatch(dataPointBatch.size());
		}
	}
}
