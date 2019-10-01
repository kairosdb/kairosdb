package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.RetryPolicy;
import com.google.inject.Inject;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;
import org.kairosdb.metrics.RetryMetrics;
import org.kairosdb.metrics4j.MetricSourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;

public class KairosRetryPolicy implements RetryPolicy//, KairosMetricReporter
{
	public static final Logger logger = LoggerFactory.getLogger(KairosRetryPolicy.class);
	private static final RetryMetrics Metrics = MetricSourceManager.getSource(RetryMetrics.class);

	private final int m_retryCount;

	//private AtomicInteger m_readRetries = new AtomicInteger(0);
	//private AtomicInteger m_writeRetries = new AtomicInteger(0);
	//private AtomicInteger m_unavailableRetries = new AtomicInteger(0);
	//private AtomicInteger m_errorRetries = new AtomicInteger(0);

	@Inject
	@Named("HOSTNAME")
	private String m_hostName = "localhost";

	@Inject
	@Named("cluster_name")
	private String m_clusterName = "cluster_name";

	@Inject
	private LongDataPointFactory m_longDataPointFactory = new LongDataPointFactoryImpl();

	@Inject
	public KairosRetryPolicy(@Named("request_retry_count") int retryCount)
	{
		m_retryCount = retryCount;
	}

	@Override
	public RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl,
			int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry)
	{
		if (nbRetry == m_retryCount)
			return RetryDecision.rethrow();
		else
		{
			Metrics.retryCount(m_hostName, m_clusterName, "read_timeout").put(1);
			return RetryDecision.tryNextHost(cl);
		}
	}

	@Override
	public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl,
			WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry)
	{
		if (nbRetry == m_retryCount)
			return RetryDecision.rethrow();
		else
		{
			Metrics.retryCount(m_hostName, m_clusterName, "write_timeout").put(1);
			return RetryDecision.tryNextHost(cl);
		}
	}

	@Override
	public RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl,
			int requiredReplica, int aliveReplica, int nbRetry)
	{
		if (nbRetry == m_retryCount)
			return RetryDecision.rethrow();
		else
		{
			Metrics.retryCount(m_hostName, m_clusterName, "unavailable").put(1);
			return RetryDecision.tryNextHost(cl);
		}
	}

	@Override
	public RetryDecision onRequestError(Statement statement, ConsistencyLevel cl,
			DriverException e, int nbRetry)
	{
		if (nbRetry == m_retryCount)
			return RetryDecision.rethrow();
		else
		{
			Metrics.retryCount(m_hostName, m_clusterName, "request_error").put(1);
			return RetryDecision.tryNextHost(cl);
		}
	}

	@Override
	public void init(Cluster cluster)
	{
		logger.info("Initializing KairosRetryPolicy: retry count set to "+m_retryCount);
	}

	@Override
	public void close()
	{
		logger.info("Closing KairosRetryPolicy");
	}

	/*@Override
	public List<DataPointSet> getMetrics(long now)
	{
		List<DataPointSet> ret = new ArrayList<>();

		Map<String, String> tags = new HashMap<>();
		tags.put("host", m_hostName);
		tags.put("cluster", m_clusterName);

		tags.put("retry_type", "read_timeout");
		ret.add(new DataPointSet("kairosdb.datastore.cassandra.retry_count", tags,
				Collections.singletonList(m_longDataPointFactory.createDataPoint(now, m_readRetries.getAndSet(0)))));

		tags.put("retry_type", "write_timeout");
		ret.add(new DataPointSet("kairosdb.datastore.cassandra.retry_count", tags,
				Collections.singletonList(m_longDataPointFactory.createDataPoint(now, m_writeRetries.getAndSet(0)))));

		tags.put("retry_type", "unavailable");
		ret.add(new DataPointSet("kairosdb.datastore.cassandra.retry_count", tags,
				Collections.singletonList(m_longDataPointFactory.createDataPoint(now, m_unavailableRetries.getAndSet(0)))));

		tags.put("retry_type", "request_error");
		ret.add(new DataPointSet("kairosdb.datastore.cassandra.retry_count", tags,
				Collections.singletonList(m_longDataPointFactory.createDataPoint(now, m_errorRetries.getAndSet(0)))));


		return ret;
	}*/
}
