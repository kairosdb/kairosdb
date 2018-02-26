package org.kairosdb.core.health;

import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.datastore.DatastoreQuery;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.exception.DatastoreException;

import static com.codahale.metrics.health.HealthCheck.Result;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DatastoreQueryHealthCheckTest
{
	private KairosDatastore datastore;
	private DatastoreQuery query;
	private DatastoreQueryHealthCheck healthCheck;

	@Before
	public void setup() throws DatastoreException
	{
		datastore = mock(KairosDatastore.class);
		query = mock(DatastoreQuery.class);
		//when(datastore.getMetricNames()).thenReturn(Collections.<String>emptyList());
		when(datastore.createQuery(any(QueryMetric.class))).thenReturn(query);

		healthCheck = new DatastoreQueryHealthCheck(datastore);
	}

	@Test(expected = NullPointerException.class)
	public void testConstructorNullDatastoreInvalid()
	{
		new DatastoreQueryHealthCheck(null);
	}

	@Test
	public void testCheckHealthy() throws Exception
	{
		Result result = healthCheck.check();

		assertTrue(result.isHealthy());
	}

	@Test
	public void testCheckUnHealthy() throws Exception
	{
		Exception exception = new DatastoreException("Error message");
		when(query.execute()).thenThrow(exception);

		Result result = healthCheck.check();

		assertFalse(result.isHealthy());
		assertThat(result.getError(), CoreMatchers.<Throwable>equalTo(exception));
		assertThat(result.getMessage(), equalTo(exception.getMessage()));
	}

	@Test
	public void testGetName()
	{
		assertThat(healthCheck.getName(), equalTo(DatastoreQueryHealthCheck.NAME));
	}
}