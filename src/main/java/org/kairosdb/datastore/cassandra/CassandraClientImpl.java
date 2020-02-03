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
import com.datastax.driver.core.TimestampGenerator;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.KairosPostConstructInit;
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
public class CassandraClientImpl implements CassandraClient, KairosMetricReporter, KairosPostConstructInit
{
	public static final Logger logger = LoggerFactory.getLogger(CassandraClientImpl.class);

	private Cluster m_cluster;
	private final String m_keyspace;
	private final String m_replication;
	private LoadBalancingPolicy m_writeLoadBalancingPolicy;

	@Inject
	@Named("HOSTNAME")
	private String m_hostName = "localhost";

	@Inject
	private LongDataPointFactory m_longDataPointFactory = new LongDataPointFactoryImpl();

	@Inject
	private DoubleDataPointFactory m_doubleDataPointFactory = new DoubleDataPointFactoryImpl();

	@Inject
	private KairosRetryPolicy m_kairosRetryPolicy = new KairosRetryPolicy(1);

	@Inject(optional=true)
	private AuthProvider m_authProvider = null;

	private final String m_clusterName;

	private final ClusterConfiguration m_clusterConfiguration;

	@Inject
	public CassandraClientImpl(ClusterConfiguration configuration)
	{
		m_clusterConfiguration = configuration;
		m_clusterName = configuration.getClusterName();

		m_keyspace = m_clusterConfiguration.getKeyspace();
		m_replication = m_clusterConfiguration.getReplication();
	}

	public void init()
	{
		//Passing shuffleReplicas = false so we can properly batch data to
		//instances.  A load balancing policy for reads will set shuffle to true
		// When connecting to Cassandra notes in different datacenters, the local datacenter should be provided.
		// Not doing this will select the datacenter from the first connected Cassandra node, which is not guaranteed to be the correct one.
		m_writeLoadBalancingPolicy = new TokenAwarePolicy((m_clusterConfiguration.getLocalDCName() == null) ? new RoundRobinPolicy() : DCAwareRoundRobinPolicy.builder().withLocalDc(m_clusterConfiguration.getLocalDCName()).build(), TokenAwarePolicy.ReplicaOrdering.TOPOLOGICAL);
		TokenAwarePolicy readLoadBalancePolicy = new TokenAwarePolicy((m_clusterConfiguration.getLocalDCName() == null) ? new RoundRobinPolicy() : DCAwareRoundRobinPolicy.builder().withLocalDc(m_clusterConfiguration.getLocalDCName()).build(), TokenAwarePolicy.ReplicaOrdering.RANDOM);

		final Cluster.Builder builder = new Cluster.Builder()
				//.withProtocolVersion(ProtocolVersion.V3)
				.withPoolingOptions(new PoolingOptions().setConnectionsPerHost(HostDistance.LOCAL,
						m_clusterConfiguration.getConnectionsLocalCore(), m_clusterConfiguration.getConnectionsLocalMax())
						.setConnectionsPerHost(HostDistance.REMOTE,
								m_clusterConfiguration.getConnectionsRemoteCore(), m_clusterConfiguration.getConnectionsRemoteMax())
						.setMaxRequestsPerConnection(HostDistance.LOCAL, m_clusterConfiguration.getRequestsPerConnectionLocal())
						.setMaxRequestsPerConnection(HostDistance.REMOTE, m_clusterConfiguration.getRequestsPerConnectionRemote())
						.setMaxQueueSize(m_clusterConfiguration.getMaxQueueSize()))
				.withReconnectionPolicy(new ExponentialReconnectionPolicy(100, 5 * 1000))
				.withLoadBalancingPolicy(new SelectiveLoadBalancingPolicy(readLoadBalancePolicy, m_writeLoadBalancingPolicy))
				.withCompression(m_clusterConfiguration.getCompression())
				.withoutJMXReporting()
				.withQueryOptions(new QueryOptions().setConsistencyLevel(m_clusterConfiguration.getReadConsistencyLevel()))
				.withTimestampGenerator(new TimestampGenerator() //todo need to remove this and put it only on the datapoints call
				{
					@Override
					public long next()
					{
						return System.currentTimeMillis();
					}
				})
				.withRetryPolicy(m_kairosRetryPolicy);

		if (m_authProvider != null)
		{
			builder.withAuthProvider(m_authProvider);
		}
		else if (m_clusterConfiguration.getAuthUser() != null && m_clusterConfiguration.getAuthPassword() != null)
		{
			builder.withCredentials(m_clusterConfiguration.getAuthUser(),
					m_clusterConfiguration.getAuthPassword());
		}


		for (Map.Entry<String, Integer> hostPort : m_clusterConfiguration.getHostList().entrySet())
		{
			logger.info("Connecting to "+hostPort.getKey()+":"+hostPort.getValue());
			builder.addContactPoint(hostPort.getKey())
					.withPort(hostPort.getValue());
		}

		if (m_clusterConfiguration.isUseSsl())
			builder.withSSL();

		m_cluster = builder.build();
	}

	public LoadBalancingPolicy getWriteLoadBalancingPolicy()
	{
		return m_writeLoadBalancingPolicy;
	}

	public ClusterConfiguration getClusterConfiguration()
	{
		return m_clusterConfiguration;
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
		dps.addTag("cluster", m_clusterName);
		dps.addDataPoint(m_longDataPointFactory.createDataPoint(now, value));

		return dps;
	}

	private DataPointSet newDataPointSet(String metricPrefix, String metricSuffix,
			long now, double value)
	{
		DataPointSet dps = new DataPointSet(new StringBuilder(metricPrefix).append(".").append(metricSuffix).toString());
		dps.addTag("host", m_hostName);
		dps.addTag("cluster", m_clusterName);
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
