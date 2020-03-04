package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import org.kairosdb.core.DataPoint;
import org.kairosdb.util.KDataOutput;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.kairosdb.datastore.cassandra.CassandraDatastore.DATA_POINTS_ROW_KEY_SERIALIZER;
import static org.kairosdb.datastore.cassandra.CassandraDatastore.ROW_KEY_METRIC_NAMES;
import static org.kairosdb.datastore.cassandra.ClusterConnection.DATA_POINTS_TABLE_NAME;

/**
 Created by bhawkins on 1/11/17.
 */
public class CQLBatch
{
	private static final Charset UTF_8 = Charset.forName("UTF-8");

	private final ClusterConnection m_clusterConnection;
	private final BatchStats m_batchStats;
	private final ConsistencyLevel m_consistencyLevel;
	private final long m_now;
	private final LoadBalancingPolicy m_loadBalancingPolicy;

	private long m_rowKeysCount = 0;
	private long m_rowKeyTimeIndexCount = 0;
	private long m_tagIndexedRowKeysCount = 0;

	private Map<Host, BatchStatement> m_batchMap = new HashMap<>();

	private BatchStatement m_metricNamesBatch = new BatchStatement(BatchStatement.Type.UNLOGGED);
	private BatchStatement m_dataPointBatch = new BatchStatement(BatchStatement.Type.UNLOGGED);
	private BatchStatement m_rowKeyBatch = new BatchStatement(BatchStatement.Type.UNLOGGED);

	private List<DataPointsRowKey> m_newRowKeys = new ArrayList<>();
	private List<String> m_newMetrics = new ArrayList<>();



	@Inject
	public CQLBatch(
			ConsistencyLevel consistencyLevel,
			@Named("write_cluster")ClusterConnection clusterConnection,
			BatchStats batchStats,
			LoadBalancingPolicy loadBalancingPolicy)
	{
		m_consistencyLevel = consistencyLevel;
		m_clusterConnection = clusterConnection;
		m_batchStats = batchStats;
		m_now = System.currentTimeMillis();
		m_loadBalancingPolicy = loadBalancingPolicy;

		m_metricNamesBatch.setConsistencyLevel(consistencyLevel);
		m_dataPointBatch.setConsistencyLevel(consistencyLevel);
		m_rowKeyBatch.setConsistencyLevel(consistencyLevel);
	}

	public void addRowKey(String metricName, DataPointsRowKey rowKey, int rowKeyTtl)
	{
		m_newRowKeys.add(rowKey);
		ByteBuffer bb = ByteBuffer.allocate(8);
		bb.putLong(0, rowKey.getTimestamp());

		Statement bs = m_clusterConnection.psRowKeyTimeInsert.bind()
				.setString(0, metricName)
				.setString(1, DATA_POINTS_TABLE_NAME)
				.setTimestamp(2, new Date(rowKey.getTimestamp()))
				//.setBytesUnsafe(1, bb) //Setting timestamp in a more optimal way
				.setInt(3, rowKeyTtl)
				.setIdempotent(true);

		bs.setConsistencyLevel(m_consistencyLevel);

		m_rowKeyBatch.add(bs);
		m_rowKeyTimeIndexCount++;

		m_rowKeysCount++;
		RowKeyLookup rowKeyLookup = m_clusterConnection.getRowKeyLookupForMetric(rowKey.getMetricName());
		List<Statement> insertStatements = rowKeyLookup.createInsertStatements(rowKey, rowKeyTtl);
		//if this is greater than 1 we are indexing on a tag
		if (insertStatements.size() > 1)
			m_tagIndexedRowKeysCount += (insertStatements.size() - 1)
					;
		for (Statement rowKeyInsertStmt : insertStatements)
		{
			rowKeyInsertStmt.setConsistencyLevel(m_consistencyLevel);
			m_rowKeyBatch.add(rowKeyInsertStmt);
		}
	}

	public void indexRowKey(DataPointsRowKey rowKey, int rowKeyTtl)
	{
		RowKeyLookup rowKeyLookup = m_clusterConnection.getRowKeyLookupForMetric(rowKey.getMetricName());
		for (Statement rowKeyInsertStmt : rowKeyLookup.createIndexStatements(rowKey, rowKeyTtl))
		{
			m_tagIndexedRowKeysCount++;
			rowKeyInsertStmt.setConsistencyLevel(m_consistencyLevel);
			m_rowKeyBatch.add(rowKeyInsertStmt);
		}
	}

	public void addMetricName(String metricName)
	{
		m_newMetrics.add(metricName);
		BoundStatement bs = new BoundStatement(m_clusterConnection.psStringIndexInsert);
		bs.setBytesUnsafe(0, ByteBuffer.wrap(ROW_KEY_METRIC_NAMES.getBytes(UTF_8)));
		bs.setString(1, metricName);
		bs.setConsistencyLevel(m_consistencyLevel);
		m_metricNamesBatch.add(bs);
	}

	private void addBoundStatement(BoundStatement boundStatement)
	{
		Iterator<Host> hosts = m_loadBalancingPolicy.newQueryPlan(m_clusterConnection.getKeyspace(), boundStatement);
		if (hosts.hasNext())
		{
			Host hostKey = hosts.next();

			BatchStatement batchStatement = m_batchMap.get(hostKey);
			if (batchStatement == null)
			{
				batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
				batchStatement.setConsistencyLevel(m_consistencyLevel);
				m_batchMap.put(hostKey, batchStatement);
			}
			batchStatement.add(boundStatement);
		}
		else
		{
			m_dataPointBatch.add(boundStatement);
		}
	}

	public void deleteDataPoint(DataPointsRowKey rowKey, int columnTime) throws IOException
	{
		BoundStatement boundStatement = new BoundStatement(m_clusterConnection.psDataPointsDelete);
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

		BoundStatement boundStatement = new BoundStatement(m_clusterConnection.psDataPointsInsert);
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
		if (m_metricNamesBatch.size() != 0)
		{
			m_clusterConnection.executeAsync(m_metricNamesBatch);
			m_batchStats.addNameBatch(m_metricNamesBatch.size());
		}

		if (m_rowKeyBatch.size() != 0)
		{
			//m_rowKeyBatch.enableTracing();
			m_clusterConnection.executeAsync(m_rowKeyBatch);
			m_batchStats.addRowKeyBatch(m_rowKeysCount);
			m_batchStats.addRowKeyTimeBatch(m_rowKeyTimeIndexCount);
			m_batchStats.addTagIndexedBatch(m_tagIndexedRowKeysCount);
		}

		for (BatchStatement batchStatement : m_batchMap.values())
		{
			//batchStatement.enableTracing();
			if (batchStatement.size() != 0)
			{
				m_clusterConnection.execute(batchStatement);
				//System.out.println(resultSet.getExecutionInfo().getQueryTrace().getTraceId());
				m_batchStats.addDatapointsBatch(batchStatement.size());
			}
		}

		//Catch all in case of a load balancing problem
		if (m_dataPointBatch.size() != 0)
		{
			m_clusterConnection.execute(m_dataPointBatch);
			m_batchStats.addDatapointsBatch(m_dataPointBatch.size());
		}
	}

	public List<DataPointsRowKey> getNewRowKeys()
	{
		return m_newRowKeys;
	}

	public List<String> getNewMetrics()
	{
		return m_newMetrics;
	}
}
