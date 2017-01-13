package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.eventbus.EventBus;
import com.google.inject.name.Named;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.queue.EventCompletionCallBack;
import org.kairosdb.events.DataPointEvent;
import org.kairosdb.util.KDataOutput;

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;

import static org.kairosdb.datastore.cassandra.CassandraClientImpl.HOST_LIST_PROPERTY;
import static org.kairosdb.datastore.cassandra.CassandraDatastore.*;

/**
 Created by bhawkins on 12/12/16.
 */
public class AstyanaxClient
{
	private ColumnFamily<DataPointsRowKey, Integer> CF_DATA_POINTS =
			new ColumnFamily<>(CF_DATA_POINTS_NAME,
					new AstyanaxDataPointsRowKeySerializer(), IntegerSerializer.get(), BytesArraySerializer.get());

	private ColumnFamily<String, DataPointsRowKey> CF_ROW_KEY_INDEX =
			new ColumnFamily<>(CF_ROW_KEY_INDEX_NAME,
					StringSerializer.get(), new AstyanaxDataPointsRowKeySerializer());

	private ColumnFamily<String, String> CF_STRING_INDEX =
			new ColumnFamily<>(CF_STRING_INDEX_NAME,
					StringSerializer.get(), StringSerializer.get());

	private Keyspace m_keyspace;

	@Inject
	public AstyanaxClient(CassandraConfiguration cassandraConfiguration,
			@Named(HOST_LIST_PROPERTY)String hostList)
	{
		AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
				.forCluster("ClusterName")
				.forKeyspace(cassandraConfiguration.getKeyspaceName())
				.withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
						.setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
				)
				.withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
						.setPort(9160)
						.setMaxConnsPerHost(10)
						.setSeeds(hostList)
				)
				.withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
				.buildKeyspace(ThriftFamilyFactory.getInstance());

		context.start();
		m_keyspace = context.getClient();
	}


	public BatchHandler getBatchHandler(List<DataPointEvent> events, EventCompletionCallBack callBack,
			int defaultTtl, DataCache<DataPointsRowKey>
			rowKeyCache, DataCache<String> metricNameCache, EventBus eventBus,
			Session session, PreparedStatement psInsertData,
			PreparedStatement psInsertRowKey, PreparedStatement psInsertString)
	{
		return new AstyanaxBatchHandler(events, callBack, defaultTtl,
				rowKeyCache, metricNameCache, eventBus, session, psInsertData,
				psInsertRowKey, psInsertString);
	}

	private class AstyanaxBatchHandler extends CQLBatchHandler
	{
		MutationBatch m_batch;

		public AstyanaxBatchHandler(List<DataPointEvent> events, EventCompletionCallBack callBack,
				int defaultTtl, DataCache<DataPointsRowKey>
				rowKeyCache, DataCache<String> metricNameCache, EventBus eventBus,
				Session session, PreparedStatement psInsertData,
				PreparedStatement psInsertRowKey, PreparedStatement psInsertString)
		{
			super(events, callBack, defaultTtl, rowKeyCache, metricNameCache, eventBus,
					session, psInsertData, psInsertRowKey, psInsertString);

			m_batch = m_keyspace.prepareMutationBatch();
		}


		@Override
		public void addMetricName(String metricName)
		{
			m_batch.withRow(CF_STRING_INDEX, ROW_KEY_METRIC_NAMES)
					.putColumn(metricName, (String)null);
		}


		@Override
		public void addDataPoint(DataPointsRowKey rowKey, int columnTime, DataPoint dataPoint, int ttl) throws IOException
		{
			KDataOutput kDataOutput = new KDataOutput();
			dataPoint.writeValueToBuffer(kDataOutput);

			m_batch.withRow(CF_DATA_POINTS, rowKey)
					.putColumn(columnTime, kDataOutput.getBytes(), ttl);
		}

		@Override
		public void submitBatch()
		{
			try
			{
				super.submitBatch();
				m_batch.execute();
			}
			catch (ConnectionException e)
			{
				e.printStackTrace();
			}
		}
	}
}
