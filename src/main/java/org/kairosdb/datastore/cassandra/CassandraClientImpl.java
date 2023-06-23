package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Metrics;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TimestampGenerator;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.kairosdb.core.KairosPostConstructInit;
import org.kairosdb.metrics4j.MetricSourceManager;
import org.kairosdb.metrics4j.annotation.Reported;
import org.kairosdb.metrics4j.annotation.Snapshot;
import org.kairosdb.metrics4j.collectors.MetricCollector;
import org.kairosdb.metrics4j.reporting.DoubleValue;
import org.kairosdb.metrics4j.reporting.MetricReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 Created by bhawkins on 3/4/15.
 */
public class CassandraClientImpl implements CassandraClient, KairosPostConstructInit
{
	public static final Logger logger = LoggerFactory.getLogger(CassandraClientImpl.class);

	private CqlSession m_cluster;
	private final String m_keyspace;
	private final String m_replication;
	private LoadBalancingPolicy m_writeLoadBalancingPolicy;


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
		//final Cluster.Builder builder = new Cluster.Builder()

		CqlSessionBuilder builder = CqlSession.builder();

		builder.withConfigLoader(new DefaultDriverConfigLoader());

		DefaultDriverOption.values();


		m_cluster = builder.build();



		Map<String, String> tags = ImmutableMap.of("cluster", m_clusterName);
		ClientMetrics clientMetrics = new ClientMetrics();
		//this reports all the @Reported annotated methods
		MetricSourceManager.addSource(clientMetrics, tags);
		//This reports for the request timer that needs a snapshot done first
		MetricSourceManager.addSource(ClientMetrics.class.getName(), "requestsTimer", tags,
				"Client requests timer", clientMetrics);
	}

	public LoadBalancingPolicy getWriteLoadBalancingPolicy()
	{
		return m_cluster.getContext().getLoadBalancingPolicy("ingest");
	}

	public ClusterConfiguration getClusterConfiguration()
	{
		return m_clusterConfiguration;
	}

	@Override
	public CqlSession getKeyspaceSession()
	{
		return m_cluster.connect(m_keyspace);
	}

	@Override
	public CqlSession getSession()
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


	public class ClientMetrics implements MetricCollector
	{
		private Metrics m_metrics;
		private com.codahale.metrics.Snapshot m_snapshot;

		public ClientMetrics()
		{

		}

		@Snapshot
		public void takeSnapshot()
		{
			m_metrics = m_cluster.getMetrics();
			m_snapshot = m_metrics.getRequestsTimer().getSnapshot();
		}

		@Reported(help = "Cleint bytes sent to Cassandra")
		public long bytesSent()
		{
			return m_metrics.getBytesSent().getCount();
		}

		@Reported(help = "Cleint bytes received from Cassandra")
		public long bytesReceived()
		{
			return m_metrics.getBytesReceived().getCount();
		}

		@Reported(help = "Client connection errors")
		public long connectionErrors()
		{
			return m_metrics.getErrorMetrics().getConnectionErrors().getCount();
		}

		@Reported(help = "Client blocking executor queue depth")
		public long blockingExecutorQueueDepth()
		{
			return m_metrics.getBlockingExecutorQueueDepth().getValue();
		}

		@Reported(help = "Number of connections to hosts")
		public long connectedToHosts()
		{
			return m_metrics.getConnectedToHosts().getValue();
		}

		@Reported(help = "Client executor queue depth")
		public long executorQueueDepth()
		{
			return m_metrics.getExecutorQueueDepth().getValue();
		}

		@Reported(help = "Number of known hosts")
		public long knownHosts()
		{
			return m_metrics.getKnownHosts().getValue();
		}

		@Reported(help = "Number of open connections")
		public long openConnections()
		{
			return m_metrics.getOpenConnections().getValue();
		}

		@Reported(help = "Queue size for reconnection scheduler")
		public long reconnectionSchedulerQueueSize()
		{
			return m_metrics.getReconnectionSchedulerQueueSize().getValue();
		}

		@Reported(help = "Queue size for task scheduler")
		public long taskSchedulerQueueSize()
		{
			return m_metrics.getTaskSchedulerQueueSize().getValue();
		}

		@Reported(help = "Number of trashed connections")
		public long trashedConnections()
		{
			return m_metrics.getTrashedConnections().getValue();
		}

		@Override
		public void reportMetric(MetricReporter metricReporter)
		{
			metricReporter.put("max", new DoubleValue(m_snapshot.getMax()));
			metricReporter.put("min", new DoubleValue(m_snapshot.getMin()));
			metricReporter.put("avg", new DoubleValue(m_snapshot.getMean()));
			metricReporter.put("count", new DoubleValue(m_snapshot.size()));
		}

		@Override
		public void setContextProperties(Map<String, String> map)
		{

		}
	}

	/*@Override
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

		ret.add(newDataPointSet(prefix, "bytes_sent", now,
				metrics.getBytesSent().getCount()));

		ret.add(newDataPointSet(prefix, "bytes_received", now,
				metrics.getBytesReceived().getCount()));

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
	}*/
}
