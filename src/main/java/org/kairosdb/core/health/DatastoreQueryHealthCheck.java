package org.kairosdb.core.health;

import com.codahale.metrics.health.HealthCheck;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.DatastoreQuery;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.exception.DatastoreException;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class DatastoreQueryHealthCheck extends HealthCheck implements HealthStatus
{
	static final String NAME = "Datastore-Query";
	private final KairosDatastore m_datastore;
	private final String m_checkMetric;

	@Inject
	public DatastoreQueryHealthCheck(KairosDatastore datastore,
			@Named("kairosdb.metric-prefix") String prefix)
	{
		m_datastore = requireNonNull(datastore);
		m_checkMetric = prefix+"jvm.thread_count";
	}

	@Override
	public String getName()
	{
		return NAME;
	}

	@Override
	protected Result check() throws Exception
	{
		try (DatastoreQuery query = m_datastore.createQuery(
				new QueryMetric(System.currentTimeMillis() - (10 * 60 * 1000),
						0, m_checkMetric)))
		{
			List<DataPointGroup> results = query.execute();
			return Result.healthy();
		}
		catch (DatastoreException e)
		{
			return Result.unhealthy(e);
		}
	}
}
