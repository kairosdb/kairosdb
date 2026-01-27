package org.kairosdb.datastore.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;
import com.google.inject.Inject;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;
import org.kairosdb.metrics4j.MetricSourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.inject.Named;

public class KairosRetryPolicy implements RetryPolicy
{
	public static final Logger logger = LoggerFactory.getLogger(KairosRetryPolicy.class);
	private static final RetryStats stats = MetricSourceManager.getSource(RetryStats.class);

	private final int m_retryCount;

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
	public RetryDecision onReadTimeout(Request request, ConsistencyLevel cl,
			int requiredResponses, int receivedResponses, boolean dataRetrieved, int retryCount)
	{
		if (retryCount >= m_retryCount)
			return RetryDecision.RETHROW;
		else
		{
			stats.retryCount(m_clusterName, "read_timeout").put(1);
			return RetryDecision.RETRY_NEXT;
		}
	}

	@Override
	public RetryDecision onWriteTimeout(Request request, ConsistencyLevel cl,
			WriteType writeType, int requiredAcks, int receivedAcks, int retryCount)
	{
		if (retryCount >= m_retryCount)
			return RetryDecision.RETHROW;
		else
		{
			stats.retryCount(m_clusterName, "write_timeout").put(1);
			return RetryDecision.RETRY_NEXT;
		}
	}

	@Override
	public RetryDecision onUnavailable(Request request, ConsistencyLevel cl,
			int requiredReplica, int aliveReplica, int retryCount)
	{
		if (retryCount >= m_retryCount)
			return RetryDecision.RETHROW;
		else
		{
			stats.retryCount(m_clusterName, "unavailable").put(1);
			return RetryDecision.RETRY_NEXT;
		}
	}

	@Override
	public RetryDecision onErrorResponse(Request request, com.datastax.oss.driver.api.core.servererrors.CoordinatorException error, int retryCount)
	{
		if (retryCount >= m_retryCount)
			return RetryDecision.RETHROW;
		else
		{
			stats.retryCount(m_clusterName, "request_error").put(1);
			return RetryDecision.RETRY_NEXT;
		}
	}

	@Override
	public RetryDecision onRequestAborted(Request request, Throwable error, int retryCount)
	{
		if (retryCount >= m_retryCount)
			return RetryDecision.RETHROW;
		else
		{
			stats.retryCount(m_clusterName, "request_aborted").put(1);
			return RetryDecision.RETRY_NEXT;
		}
	}

	public void init(DriverContext context)
	{
		logger.info("Initializing KairosRetryPolicy: retry count set to "+m_retryCount);
	}

	@Override
	public void close()
	{
		logger.info("Closing KairosRetryPolicy");
	}

}
