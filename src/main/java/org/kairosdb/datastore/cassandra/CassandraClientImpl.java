package org.kairosdb.datastore.cassandra;

import com.codahale.metrics.*;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.kairosdb.core.KairosPostConstructInit;
import org.kairosdb.metrics4j.MetricSourceManager;
import org.kairosdb.metrics4j.annotation.Key;
import org.kairosdb.metrics4j.annotation.Reported;
import org.kairosdb.metrics4j.annotation.Snapshot;
import org.kairosdb.metrics4j.collectors.LongCollector;
import org.kairosdb.metrics4j.collectors.MetricCollector;
import org.kairosdb.metrics4j.reporting.DoubleValue;
import org.kairosdb.metrics4j.reporting.MetricReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;

/**
 Created by bhawkins on 3/4/15.
 */
public class CassandraClientImpl implements CassandraClient, KairosPostConstructInit
{
	public static final Logger logger = LoggerFactory.getLogger(CassandraClientImpl.class);
	public static final NodeMetrics s_nodeMetrics = MetricSourceManager.getSource(NodeMetrics.class);
	public static final Meter EMPTY_METER = new Meter();
	public static final Gauge<Integer> EMPTY_GAUGE = () -> 0;

	private CqlSession m_session;
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

		builder.withConfigLoader(new DefaultDriverConfigLoader(m_clusterConfiguration::getRawConfig));

		DefaultDriverOption.values();


		m_session = builder.build();



		Map<String, String> tags = ImmutableMap.of("cluster", m_clusterName);
		SessionMetrics clientMetrics = new SessionMetrics();
		//this reports all the @Reported annotated methods
		MetricSourceManager.addSource(clientMetrics, tags);
		//This reports for the request timer that needs a snapshot done first
		MetricSourceManager.addSource(SessionMetrics.class.getName(), "requestsTimer", tags,
				"Client requests timer", clientMetrics);
	}

	public LoadBalancingPolicy getWriteLoadBalancingPolicy()
	{
		return m_session.getContext().getLoadBalancingPolicy("ingest");
	}

	public ClusterConfiguration getClusterConfiguration()
	{
		return m_clusterConfiguration;
	}

	@Override
	public CqlSession getKeyspaceSession()
	{
		return m_session.connect(m_keyspace);
	}

	@Override
	public CqlSession getSession()
	{
		return m_session;
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
		m_session.close();
	}

	public interface NodeMetrics
	{
		LongCollector connections(@Key("cluster")String cluster, @Key("node")String node);
		LongCollector writeTimeouts(@Key("cluster")String cluster, @Key("node")String node);
		LongCollector retries(@Key("cluster")String cluster, @Key("node")String node);
	}


	public class SessionMetrics implements MetricCollector
	{
		private Metrics m_metrics;
		private com.codahale.metrics.Snapshot m_snapshot;


		public SessionMetrics()
		{

		}

		@Snapshot
		public void takeSnapshot()
		{
			m_metrics = m_session.getMetrics().get();
			Map<UUID, Node> nodes = m_session.getMetadata().getNodes();//some metrics we need to get from the nodes and we will need to tag those metrics
			for (Map.Entry<UUID, Node> nodeEntry : nodes.entrySet()) {
				m_metrics.getNodeMetric(nodeEntry.getValue(), DefaultNodeMetric.OPEN_CONNECTIONS).ifPresent(metric ->
						s_nodeMetrics.connections(m_clusterName, nodeEntry.getKey().toString()).put(((Gauge<Integer>)metric).getValue()));

				m_metrics.getNodeMetric(nodeEntry.getValue(), DefaultNodeMetric.WRITE_TIMEOUTS).ifPresent(metric ->
						s_nodeMetrics.writeTimeouts(m_clusterName, nodeEntry.getKey().toString()).put(((Counter)metric).getCount()));

				m_metrics.getNodeMetric(nodeEntry.getValue(), DefaultNodeMetric.RETRIES).ifPresent(metric ->
						s_nodeMetrics.retries(m_clusterName, nodeEntry.getKey().toString()).put(((Counter)metric).getCount()));


			}

			Timer metric = (Timer) m_metrics.getSessionMetric(DefaultSessionMetric.CQL_REQUESTS).get();
			m_snapshot = metric.getSnapshot();
		}

		@Reported(help = "Client bytes sent to Cassandra")
		public long bytesSent()
		{
			Meter sessionMetric = (Meter) m_metrics.getSessionMetric(DefaultSessionMetric.BYTES_SENT).orElse(EMPTY_METER);
			return sessionMetric.getCount();
		}

		@Reported(help = "Client bytes received from Cassandra")
		public long bytesReceived()
		{
			Meter sessionMetric = (Meter) m_metrics.getSessionMetric(DefaultSessionMetric.BYTES_RECEIVED).orElse(EMPTY_METER);
			return sessionMetric.getCount();
		}

		@Reported(help = "Number of known hosts")
		public long knownHosts()
		{
			Gauge<Integer> sessionMetric = (Gauge<Integer>) m_metrics.getSessionMetric(DefaultSessionMetric.CONNECTED_NODES).orElse(EMPTY_GAUGE);
			return sessionMetric.getValue();
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
