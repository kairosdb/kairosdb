package org.kairosdb.datastore.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metadata.Node;
import org.kairosdb.core.DataPoint;
import org.kairosdb.metrics4j.MetricSourceManager;
import org.kairosdb.core.annotation.InjectProperty;
import org.kairosdb.util.KDataOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.*;

import static org.kairosdb.datastore.cassandra.CassandraDatastore.DATA_POINTS_ROW_KEY_SERIALIZER;
import static org.kairosdb.datastore.cassandra.CassandraDatastore.ROW_KEY_METRIC_NAMES;
import static org.kairosdb.datastore.cassandra.ClusterConnection.DATA_POINTS_TABLE_NAME;

/**
 Created by bhawkins on 1/11/17.
 */
public class CQLBatch
{
	private static final BatchMetrics metrics = MetricSourceManager.getSource(BatchMetrics.class);
	public static final Logger logger = LoggerFactory.getLogger(CQLBatch.class);
	private static final Charset UTF_8 = Charset.forName("UTF-8");
	public static final String METRIC_INDEX_FILTER_PREFIX = "kairosdb.metric_index_filter.prefix";

	private final ClusterConnection m_clusterConnection;
	//private final BatchStats m_batchStats;
	private final ConsistencyLevel m_consistencyLevel;
	private final long m_now;
	private final LoadBalancingPolicy m_loadBalancingPolicy;

	private long m_rowKeysCount = 0;
	private long m_rowKeyTimeIndexCount = 0;
	private long m_tagIndexedRowKeysCount = 0;

	private Map<Node, BatchStatement> m_batchMap = new HashMap<>();

	private BatchStatement m_metricNamesBatch = BatchStatement.newInstance(BatchType.UNLOGGED);
	private BatchStatement m_dataPointBatch = BatchStatement.newInstance(BatchType.UNLOGGED);
	private BatchStatement m_rowKeyBatch = BatchStatement.newInstance(BatchType.UNLOGGED);

	private List<DataPointsRowKey> m_newRowKeys = new ArrayList<>();
	private List<TimedString> m_newMetrics = new ArrayList<>();

	private List<String> m_prefixFilterList = new ArrayList<>();


	@Inject
	public CQLBatch(
			ConsistencyLevel consistencyLevel,
			@Named("write_cluster")ClusterConnection clusterConnection,
			LoadBalancingPolicy loadBalancingPolicy)
	{
		m_consistencyLevel = consistencyLevel;
		m_clusterConnection = clusterConnection;
		//m_batchStats = batchStats;
		m_now = System.currentTimeMillis();
		m_loadBalancingPolicy = loadBalancingPolicy;

		m_metricNamesBatch.setConsistencyLevel(consistencyLevel);
		m_dataPointBatch.setConsistencyLevel(consistencyLevel);
		m_rowKeyBatch.setConsistencyLevel(consistencyLevel);
	}

	@InjectProperty(prop = METRIC_INDEX_FILTER_PREFIX, optional = true)
	public void setFilterPrefixList(List<String> list)
	{
		m_prefixFilterList = list;
	}

	public void addTimeIndex(String metricName, long rowKeyTime, int rowKeyTtl)
	{
		BoundStatement bs = m_clusterConnection.psRowKeyTimeInsert.boundStatementBuilder()
				.setString(0, metricName)
				.setString(1, DATA_POINTS_TABLE_NAME)
				.setInstant(2, Instant.ofEpochMilli(rowKeyTime))
				.setInt(3, rowKeyTtl)
				.setIdempotence(true)
				.build();

		bs.setConsistencyLevel(m_consistencyLevel);

		m_rowKeyBatch.add(bs);
		m_rowKeyTimeIndexCount++;
	}

	public void addRowKey(DataPointsRowKey rowKey, int rowKeyTtl)
	{
		m_newRowKeys.add(rowKey);

		m_rowKeysCount++;
		RowKeyLookup rowKeyLookup = m_clusterConnection.getRowKeyLookupForMetric(rowKey.getMetricName());
		List<BatchableStatement> insertStatements = rowKeyLookup.createInsertStatements(rowKey, rowKeyTtl);
		//if this is greater than 1 we are indexing on a tag
		if (insertStatements.size() > 1)
			m_tagIndexedRowKeysCount += (insertStatements.size() - 1)
					;
		for (BatchableStatement rowKeyInsertStmt : insertStatements)
		{
			rowKeyInsertStmt.setConsistencyLevel(m_consistencyLevel);
			m_rowKeyBatch.add(rowKeyInsertStmt);
		}
	}

	public void indexRowKey(DataPointsRowKey rowKey, int rowKeyTtl)
	{
		RowKeyLookup rowKeyLookup = m_clusterConnection.getRowKeyLookupForMetric(rowKey.getMetricName());
		for (BatchableStatement rowKeyInsertStmt : rowKeyLookup.createIndexStatements(rowKey, rowKeyTtl))
		{
			m_tagIndexedRowKeysCount++;
			rowKeyInsertStmt.setConsistencyLevel(m_consistencyLevel);
			m_rowKeyBatch.add(rowKeyInsertStmt);
		}
	}

	public void addMetricName(TimedString metricNameTime)
	{
		String metricName = metricNameTime.getString();
		boolean skip = false;

		for (String prefix : m_prefixFilterList)
		{
			if (metricName.startsWith(prefix)) {
				skip = true;
				break;
			}
		}

		if (!skip)
		{
			m_newMetrics.add(metricNameTime);
			BoundStatement bs = m_clusterConnection.psStringIndexInsert.boundStatementBuilder()
					.setBytesUnsafe(0, ByteBuffer.wrap(ROW_KEY_METRIC_NAMES.getBytes(UTF_8)))
					.setString(1, metricName)
					.setConsistencyLevel(m_consistencyLevel)
					.setIdempotence(true)
					.build();
			m_metricNamesBatch.add(bs);
		}
	}

	private void addBoundStatement(BoundStatement boundStatement)
	{
		Iterator<Node> hosts = m_loadBalancingPolicy.newQueryPlan(m_clusterConnection.getKeyspace(), boundStatement);
		if (hosts.hasNext())
		{
			Node hostKey = hosts.next();

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
		ByteBuffer b = ByteBuffer.allocate(4);
		b.putInt(columnTime);
		b.rewind();
		BoundStatement boundStatement = m_clusterConnection.psDataPointsDelete.boundStatementBuilder()
				.setBytesUnsafe(0, DATA_POINTS_ROW_KEY_SERIALIZER.toByteBuffer(rowKey))
				.setBytesUnsafe(1, b)
				.setConsistencyLevel(m_consistencyLevel)
				.setIdempotence(true)
				.build();

		addBoundStatement(boundStatement);
	}

	public void addDataPoint(DataPointsRowKey rowKey, int columnTime,
			DataPoint dataPoint, int ttl) throws IOException
	{
		KDataOutput kDataOutput = new KDataOutput();
		dataPoint.writeValueToBuffer(kDataOutput);

		ByteBuffer b = ByteBuffer.allocate(4);
		b.putInt(columnTime);
		b.rewind();
		BoundStatement boundStatement = m_clusterConnection.psDataPointsInsert.boundStatementBuilder()
				.setBytesUnsafe(0, DATA_POINTS_ROW_KEY_SERIALIZER.toByteBuffer(rowKey))
				.setBytesUnsafe(1, b)
				.setBytesUnsafe(2, ByteBuffer.wrap(kDataOutput.getBytes()))
				.setInt(3, ttl)
				.setLong(4, m_now)
				.setConsistencyLevel(m_consistencyLevel)
				.setIdempotence(true)
				.build();


		addBoundStatement(boundStatement);
	}

	public void submitBatch()
	{
		if (m_metricNamesBatch.size() != 0)
		{
			m_clusterConnection.executeAsync(m_metricNamesBatch);
			metrics.writeBatchSize("string_index").put(m_metricNamesBatch.size());
		}

		if (m_rowKeyBatch.size() != 0)
		{
			m_clusterConnection.executeAsync(m_rowKeyBatch);
			metrics.writeBatchSize("row_keys").put(m_rowKeyBatch.size());
			metrics.writeBatchSize("row_key_time_index").put(m_rowKeyTimeIndexCount);
			metrics.writeBatchSize("tag_indexed_row_keys").put(m_tagIndexedRowKeysCount);
		}

		for (BatchStatement batchStatement : m_batchMap.values())
		{
			//batchStatement.enableTracing();
			if (batchStatement.size() != 0)
			{
				m_clusterConnection.execute(batchStatement);
				//System.out.println(resultSet.getExecutionInfo().getQueryTrace().getTraceId());
				metrics.writeBatchSize("data_points").put(batchStatement.size());
			}
		}

		//Catch all in case of a load balancing problem
		if (m_dataPointBatch.size() != 0)
		{
			m_clusterConnection.execute(m_dataPointBatch);
			metrics.writeBatchSize("data_points").put(m_dataPointBatch.size());
		}
	}

	public List<DataPointsRowKey> getNewRowKeys()
	{
		return m_newRowKeys;
	}

	public List<TimedString> getNewMetrics()
	{
		return m_newMetrics;
	}
}
