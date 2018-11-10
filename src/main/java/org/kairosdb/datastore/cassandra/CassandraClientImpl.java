package org.kairosdb.datastore.cassandra;

import com.codahale.metrics.Snapshot;
import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Metrics;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.TimestampGenerator;
import com.datastax.driver.core.policies.*;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.datapoints.DoubleDataPointFactoryImpl;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;
import org.kairosdb.core.reporting.KairosMetricReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 Created by bhawkins on 3/4/15.
 */
public class CassandraClientImpl implements CassandraClient, KairosMetricReporter
{
	public static final Logger logger = LoggerFactory.getLogger(CassandraClientImpl.class);

	private Cluster m_cluster;
	private final String m_keyspace;
	private final String m_replication;
	private LoadBalancingPolicy m_writeLoadBalancingPolicy;
	private final CassandraConfiguration m_configuration;

	@Inject
	@Named("HOSTNAME")
	private String m_hostName = "localhost";

	@Inject
	private LongDataPointFactory m_longDataPointFactory = new LongDataPointFactoryImpl();

	@Inject
	private DoubleDataPointFactory m_doubleDataPointFactory = new DoubleDataPointFactoryImpl();

	@Inject
	private RetryPolicy m_retryPolicy = new KairosRetryPolicy(1);

	@Inject(optional=true)
	private AuthProvider m_authProvider = null;

	@Inject
	public CassandraClientImpl(CassandraConfiguration configuration)
	{
		m_configuration = configuration;
		m_keyspace = configuration.getKeyspaceName();
		m_replication = configuration.getReplication();
	}

	public void init()
	{
		//Passing shuffleReplicas = false so we can properly batch data to
		//instances.
		// When connecting to Cassandra notes in different datacenters, the local datacenter should be provided.
		// Not doing this will select the datacenter from the first connected Cassandra node, which is not guaranteed to be the correct one.
		m_writeLoadBalancingPolicy = new TokenAwarePolicy((m_configuration.getLocalDatacenter() == null) ? new RoundRobinPolicy() : DCAwareRoundRobinPolicy.builder().withLocalDc(m_configuration.getLocalDatacenter()).build(), false);
		TokenAwarePolicy readLoadBalancePolicy = new TokenAwarePolicy((m_configuration.getLocalDatacenter() == null) ? new RoundRobinPolicy() : DCAwareRoundRobinPolicy.builder().withLocalDc(m_configuration.getLocalDatacenter()).build(), true);
		final Cluster.Builder builder = new Cluster.Builder()
				.withSocketOptions(new SocketOptions().setConnectTimeoutMillis(m_configuration.getConnectionTimeout())
						.setReadTimeoutMillis(m_configuration.getReadTimeout()))
				.withPoolingOptions(new PoolingOptions().setConnectionsPerHost(HostDistance.LOCAL,
						m_configuration.getLocalCoreConnections(), m_configuration.getLocalMaxConnections())
						.setConnectionsPerHost(HostDistance.REMOTE,
								m_configuration.getRemoteCoreConnections(), m_configuration.getRemoteMaxConnections())
					.setMaxRequestsPerConnection(HostDistance.LOCAL, m_configuration.getLocalMaxReqPerConn())
					.setMaxRequestsPerConnection(HostDistance.REMOTE, m_configuration.getRemoteMaxReqPerConn())
					.setMaxQueueSize(m_configuration.getMaxQueueSize()))
				.withReconnectionPolicy(new ExponentialReconnectionPolicy(100, 5 * 1000))
				.withLoadBalancingPolicy(new SelectiveLoadBalancingPolicy(readLoadBalancePolicy, m_writeLoadBalancingPolicy))
				.withCompression(ProtocolOptions.Compression.LZ4)
				.withoutJMXReporting()
				.withQueryOptions(new QueryOptions().setConsistencyLevel(m_configuration.getDataReadLevel()))
				.withTimestampGenerator(new TimestampGenerator() //todo need to remove this and put it only on the datapoints call
				{
					@Override
					public long next()
					{
						return System.currentTimeMillis();
					}
				})
				.withRetryPolicy(m_retryPolicy);

		if (m_authProvider != null)
		{
			builder.withAuthProvider(m_authProvider);
		}
		else if (m_configuration.getAuthUserName() != null && m_configuration.getAuthPassword() != null)
		{
			builder.withCredentials(m_configuration.getAuthUserName(),
					m_configuration.getAuthPassword());
		}


		for (Map.Entry<String, Integer> hostPort : m_configuration.getHostList().entrySet())
		{
			logger.info("Connecting to "+hostPort.getKey()+":"+hostPort.getValue());
			builder.addContactPoint(hostPort.getKey())
					.withPort(hostPort.getValue());
		}

		if (m_configuration.isUseSsl())
			builder.withSSL();

		m_cluster = builder.build();

	}

	public LoadBalancingPolicy getWriteLoadBalancingPolicy()
	{
		return m_writeLoadBalancingPolicy;
	}

	@Override
	public Session getKeyspaceSession()
	{
		return m_cluster.connect(m_keyspace);
	}

	@Override
	public Session getSession()
	{
		return m_cluster.connect();
	}

	@Override
	public String getKeyspace()
	{
		return m_keyspace;
	}

	@Override
	public String getReplication() { return m_replication; }

	@Override
	public void close()
	{
		m_cluster.close();
	}


	private DataPointSet newDataPointSet(String metricPrefix, String metricSuffix,
			long now, long value)
	{
		DataPointSet dps = new DataPointSet(new StringBuilder(metricPrefix).append(".").append(metricSuffix).toString());
		dps.addTag("host", m_hostName);
		dps.addDataPoint(m_longDataPointFactory.createDataPoint(now, value));

		return dps;
	}

	private DataPointSet newDataPointSet(String metricPrefix, String metricSuffix,
			long now, double value)
	{
		DataPointSet dps = new DataPointSet(new StringBuilder(metricPrefix).append(".").append(metricSuffix).toString());
		dps.addTag("host", m_hostName);
		dps.addDataPoint(m_doubleDataPointFactory.createDataPoint(now, value));

		return dps;
	}

	@Override
	public List<DataPointSet> getMetrics(long now)
	{
		String prefix = "kairosdb.datastore.cassandra.client";
		List<DataPointSet> ret = new ArrayList<>();
		Metrics metrics = m_cluster.getMetrics();

		ret.add(newDataPointSet(prefix, "connection_errors", now,
				metrics.getErrorMetrics().getConnectionErrors().getCount()));

		ret.add(newDataPointSet(prefix, "blocking_executor_queue_depth", now,
				metrics.getBlockingExecutorQueueDepth().getValue()));

		ret.add(newDataPointSet(prefix, "connected_to_hosts", now,
				metrics.getConnectedToHosts().getValue()));

		ret.add(newDataPointSet(prefix, "executor_queue_depth", now,
				metrics.getExecutorQueueDepth().getValue()));

		ret.add(newDataPointSet(prefix, "known_hosts", now,
				metrics.getKnownHosts().getValue()));

		ret.add(newDataPointSet(prefix, "open_connections", now,
				metrics.getOpenConnections().getValue()));

		ret.add(newDataPointSet(prefix, "reconnection_scheduler_queue_size", now,
				metrics.getReconnectionSchedulerQueueSize().getValue()));

		ret.add(newDataPointSet(prefix, "task_scheduler_queue_size", now,
				metrics.getTaskSchedulerQueueSize().getValue()));

		ret.add(newDataPointSet(prefix, "trashed_connections", now,
				metrics.getTrashedConnections().getValue()));

		Snapshot snapshot = metrics.getRequestsTimer().getSnapshot();
		prefix = prefix + ".requests_timer";
		ret.add(newDataPointSet(prefix, "max", now,
				snapshot.getMax()));

		ret.add(newDataPointSet(prefix, "min", now,
				snapshot.getMin()));

		ret.add(newDataPointSet(prefix, "avg", now,
				snapshot.getMean()));

		ret.add(newDataPointSet(prefix, "count", now,
				snapshot.size()));

		return ret;
	}
}
