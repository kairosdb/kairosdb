package org.kairosdb.core.health;

import com.codahale.metrics.health.HealthCheck;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.http.rest.MetricsResource;

import javax.ws.rs.*;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.kairosdb.core.http.rest.MetricsResource.setHeaders;

/**
 Provides REST APIs for health checks
 */
@Path("/api/v1/health")
public class HealthCheckResource
{
	private final HealthCheckService m_healthCheckService;

	@Inject
	@Named("kairosdb.health.healthyResponseCode")
	private int m_healthyResponse = Response.Status.NO_CONTENT.getStatusCode();


	@Inject
	public HealthCheckResource(HealthCheckService healthCheckService)
	{
		m_healthCheckService = checkNotNull(healthCheckService);
	}

	@OPTIONS
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("check")
	public Response corsPreflightCheck(@HeaderParam("Access-Control-Request-Headers") String requestHeaders,
			@HeaderParam("Access-Control-Request-Method") String requestMethod)
	{
		Response.ResponseBuilder responseBuilder = MetricsResource.getCorsPreflightResponseBuilder(requestHeaders, requestMethod);
		return (responseBuilder.build());
	}

	/**
	 Health check

	 @return 204 if healthy otherwise 500
	 */
	@GET
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("check")
	public Response check()
	{
		for (HealthStatus healthCheck : m_healthCheckService.getChecks())
		{
			HealthCheck.Result result = healthCheck.execute();
			if (!result.isHealthy())
			{
				return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR)).build();
			}
		}

		return setHeaders(Response.status(m_healthyResponse)).build();
	}


	@OPTIONS
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("status")
	public Response corsPreflightStatus(@HeaderParam("Access-Control-Request-Headers") String requestHeaders,
			@HeaderParam("Access-Control-Request-Method") String requestMethod)
	{
		Response.ResponseBuilder responseBuilder = MetricsResource.getCorsPreflightResponseBuilder(requestHeaders, requestMethod);
		return (responseBuilder.build());
	}


	/**
	 Returns the status of each health check.

	 @return 200
	 */
	@GET
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("status")
	public Response status()
	{
		List<String> messages = new ArrayList<String>();
		for (HealthStatus healthCheck : m_healthCheckService.getChecks())
		{
			HealthCheck.Result result = healthCheck.execute();
			if (result.isHealthy())
			{
				messages.add(healthCheck.getName() + ": OK");
			}
			else
			{
				messages.add(healthCheck.getName() + ": FAIL");
			}
		}

		GenericEntity<List<String>> entity = new GenericEntity<List<String>>(messages)
		{
		};
		return setHeaders(Response.ok(entity)).build();
	}

}
