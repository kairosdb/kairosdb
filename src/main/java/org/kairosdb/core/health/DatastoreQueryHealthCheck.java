package org.kairosdb.core.health;

import com.codahale.metrics.health.HealthCheck;
import com.google.inject.Inject;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.exception.DatastoreException;

import static com.google.common.base.Preconditions.checkNotNull;

public class DatastoreQueryHealthCheck extends HealthCheck implements HealthStatus
{
	static final String NAME = "Datastore-Query";
	private final Datastore datastore;

	@Inject
	public DatastoreQueryHealthCheck(Datastore datastore)
	{
		this.datastore = checkNotNull(datastore);
	}

	@Override
	public String getName()
	{
		return NAME;
	}

	@Override
	protected Result check() throws Exception
	{
		try
		{
			datastore.getMetricNames();
			return Result.healthy();
		}
		catch (DatastoreException e)
		{
			return Result.unhealthy(e);
		}
	}
}
