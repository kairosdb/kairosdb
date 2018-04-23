package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.RetryPolicy;

public class KairosRetryPolicy implements RetryPolicy
{
	private final int m_retryCount;

	public KairosRetryPolicy(int retryCount)
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
			return RetryDecision.retry(cl);
	}

	@Override
	public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl,
			WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry)
	{
		if (nbRetry == m_retryCount)
			return RetryDecision.rethrow();
		else
			return RetryDecision.retry(cl);
	}

	@Override
	public RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl,
			int requiredReplica, int aliveReplica, int nbRetry)
	{
		if (nbRetry == m_retryCount)
			return RetryDecision.rethrow();
		else
			return RetryDecision.retry(cl);
	}

	@Override
	public RetryDecision onRequestError(Statement statement, ConsistencyLevel cl,
			DriverException e, int nbRetry)
	{
		if (nbRetry == m_retryCount)
			return RetryDecision.rethrow();
		else
			return RetryDecision.retry(cl);
	}

	@Override
	public void init(Cluster cluster)
	{

	}

	@Override
	public void close()
	{

	}
}
