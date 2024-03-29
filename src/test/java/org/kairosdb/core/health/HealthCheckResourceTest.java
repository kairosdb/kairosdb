package org.kairosdb.core.health;

import com.codahale.metrics.health.HealthCheck;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.datastore.DatastoreQuery;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.exception.DatastoreException;

import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HealthCheckResourceTest
{
	private HealthCheckResource resourceService;
	private KairosDatastore datastore;
	private DatastoreQuery query;

	@Before
	public void setup() throws DatastoreException
	{
		datastore = mock(KairosDatastore.class);
		query = mock(DatastoreQuery.class);
		when(datastore.getMetricNames(null)).thenReturn(Collections.<String>emptyList());
		when(datastore.createQuery(any(QueryMetric.class))).thenReturn(query);

		HealthCheckService healthCheckService = new TestHealthCheckService();
		resourceService = new HealthCheckResource(healthCheckService);
	}

	@Test(expected = NullPointerException.class)
	public void testConstructorNullHealthCheckServiceInvalid()
	{
		new HealthCheckResource(null);
	}

	@Test
	public void testCheckAllHealthy()
	{
		Response response = resourceService.check();

		assertThat(response.getStatus(), equalTo(Response.Status.NO_CONTENT.getStatusCode()));
	}

	@Test
	public void testCheckUnHealthy() throws DatastoreException
	{
		when(query.execute()).thenThrow(new DatastoreException("Error"));
		Response response = resourceService.check();

		assertThat(response.getStatus(), equalTo(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()));
	}

	@Test
	public void testStatusAllHealthy()
	{
		Response response = resourceService.status();

		assertThat(response.getStatus(), equalTo(Response.Status.OK.getStatusCode()));
	}

	@Test
	public void testStatusUnHealthy() throws DatastoreException
	{
		when(datastore.getMetricNames(null)).thenThrow(new DatastoreException("Error"));
		Response response = resourceService.status();

		assertThat(response.getStatus(), equalTo(Response.Status.OK.getStatusCode()));
	}

	private class TestHealthCheckService implements HealthCheckService
	{
		@Override
		public List<HealthStatus> getChecks()
		{
			List<HealthStatus> list = new ArrayList<HealthStatus>();
			list.add(new TestHealthStatus());
			list.add(new DatastoreQueryHealthCheck(datastore, "kairosdb."));

			return list;
		}
	}

	private class TestHealthStatus implements HealthStatus
	{
		@Override
		public String getName()
		{
			return getClass().getSimpleName();
		}

		@Override
		public HealthCheck.Result execute()
		{
			return HealthCheck.Result.healthy();
		}
	}
}