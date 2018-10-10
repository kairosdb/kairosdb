package org.kairosdb.datastore.remote;

import com.codahale.metrics.health.HealthCheck;
import com.google.inject.Inject;
import org.kairosdb.core.health.HealthStatus;

import static com.google.common.base.Preconditions.checkNotNull;

public class RemoteDatastoreHealthCheck extends HealthCheck implements HealthStatus
{
	private static final String NAME = "RemoteDatastore";

	private RemoteSendJob sendJob;

	@Inject
	public RemoteDatastoreHealthCheck(RemoteSendJob sendJob)
	{
		this.sendJob = checkNotNull(sendJob, "sendJob must not be null");
	}

	@Override
	protected Result check() throws Exception
	{
		Exception exception = sendJob.getCurrentException();
		if (exception == null)
		{
			return Result.healthy();
		}
		else
		{
			return Result.unhealthy(exception);
		}
	}

	@Override
	public String getName()
	{
		return NAME;
	}
}
