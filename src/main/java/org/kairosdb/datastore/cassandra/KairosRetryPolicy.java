package org.kairosdb.datastore.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.retry.RetryVerdict;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;
import com.google.inject.Inject;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;
import org.kairosdb.metrics4j.MetricSourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;

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
	public RetryDecision onReadTimeout(
			@NonNull Request request,
			@NonNull ConsistencyLevel cl,
			int blockFor,
			int received,
			boolean dataPresent,
			int retryCount)
	{
		if (retryCount == m_retryCount)
			return RetryDecision.RETHROW;
		else
		{
			stats.retryCount(m_clusterName, "read_timeout").put(1);
			return RetryDecision.RETRY_NEXT;
		}
	}

	@Override
	public RetryDecision onWriteTimeout(@NonNull Request request, @NonNull ConsistencyLevel cl, @NonNull WriteType writeType, int blockFor, int received, int retryCount)
	{
		if (retryCount == m_retryCount)
			return RetryDecision.RETHROW;
		else
		{
			stats.retryCount(m_clusterName, "write_timeout").put(1);
			return RetryDecision.RETRY_NEXT;
		}
	}

	@Override
	public RetryDecision onUnavailable(@NonNull Request request, @NonNull ConsistencyLevel cl, int required, int alive, int retryCount)
	{
		if (retryCount == m_retryCount)
			return RetryDecision.RETHROW;
		else
		{
			stats.retryCount(m_clusterName, "unavailable").put(1);
			return RetryDecision.RETRY_NEXT;
		}
	}

	@Override
	public RetryVerdict onReadTimeoutVerdict(@NonNull Request request, @NonNull ConsistencyLevel cl, int blockFor, int received, boolean dataPresent, int retryCount)
	{
		return RetryPolicy.super.onReadTimeoutVerdict(request, cl, blockFor, received, dataPresent, retryCount);
	}



	@Override
	public RetryVerdict onWriteTimeoutVerdict(@NonNull Request request, @NonNull ConsistencyLevel cl, @NonNull WriteType writeType, int blockFor, int received, int retryCount)
	{
		return RetryPolicy.super.onWriteTimeoutVerdict(request, cl, writeType, blockFor, received, retryCount);
	}



	@Override
	public RetryVerdict onUnavailableVerdict(@NonNull Request request, @NonNull ConsistencyLevel cl, int required, int alive, int retryCount)
	{
		return RetryPolicy.super.onUnavailableVerdict(request, cl, required, alive, retryCount);
	}

	@Override
	public RetryDecision onRequestAborted(@NonNull Request request, @NonNull Throwable error, int retryCount)
	{
		return null;
	}

	@Override
	public RetryVerdict onRequestAbortedVerdict(@NonNull Request request, @NonNull Throwable error, int retryCount)
	{
		return RetryPolicy.super.onRequestAbortedVerdict(request, error, retryCount);
	}

	@Override
	public RetryDecision onErrorResponse(@NonNull Request request, @NonNull CoordinatorException error, int retryCount)
	{
		if (retryCount == m_retryCount)
			return RetryDecision.RETHROW;
		else
		{
			stats.retryCount(m_clusterName, "error_reponse").put(1);
			return RetryDecision.RETRY_NEXT;
		}
	}

	@Override
	public RetryVerdict onErrorResponseVerdict(@NonNull Request request, @NonNull CoordinatorException error, int retryCount)
	{
		return RetryPolicy.super.onErrorResponseVerdict(request, error, retryCount);
	}

	@Override
	public void close()
	{
		logger.info("Closing KairosRetryPolicy");
	}

}
