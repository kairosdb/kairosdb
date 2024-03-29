package org.kairosdb.core.http.rest;

import com.google.inject.Inject;
import org.h2.util.StringUtils;
import org.kairosdb.core.datastore.ServiceKeyStore;
import org.kairosdb.core.datastore.ServiceKeyValue;
import org.kairosdb.core.formatter.JsonFormatter;
import org.kairosdb.core.http.rest.MetricsResource.ValuesStreamingOutput;
import org.kairosdb.core.http.rest.json.ErrorResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import static java.util.Objects.requireNonNull;
import static org.kairosdb.core.http.rest.MetricsResource.setHeaders;

@Path("/api/v1/metadata")
public class MetadataResource
{
	private static final Logger logger = LoggerFactory.getLogger(MetadataResource.class);

	private final ServiceKeyStore m_keyStore;
	private final JsonFormatter jsonFormatter = new JsonFormatter();

	@SuppressWarnings("ConstantConditions")
	@Inject
	public MetadataResource(ServiceKeyStore keyStore)
	{
		this.m_keyStore = requireNonNull(keyStore, "m_keyStore cannot be null");
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/{service}")
	public Response listServiceKeys(@PathParam("service") String service)
	{
		try
		{
			checkLocalService(service);
			Iterable<String> keys = m_keyStore.listServiceKeys(service);
			ResponseBuilder responseBuilder = Response.status(Status.OK).entity(
					new ValuesStreamingOutput(jsonFormatter, keys, null));
			setHeaders(responseBuilder);
			return responseBuilder.build();
		}
		catch (NotAuthorizedException e)
		{
			logger.error("Attempt to access a local service.");
			return setHeaders(Response.status(Status.UNAUTHORIZED)).build();
		}
		catch (Exception e)
		{
			logger.error("Failed to get keys.", e);
			return setHeaders(Response.status(Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
		}
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/{service}/{serviceKey}")
	public Response listKeys(@PathParam("service") String service,
			@PathParam("serviceKey") String serviceKey, @QueryParam("startsWith") String startsWidth)
	{
		try
		{
			checkLocalService(service);
			Iterable<String> keys;
			keys = StringUtils.isNullOrEmpty(startsWidth) ?
					m_keyStore.listKeys(service, serviceKey) :
					m_keyStore.listKeys(service, serviceKey, startsWidth);

			ResponseBuilder responseBuilder = Response.status(Status.OK).entity(
					new ValuesStreamingOutput(jsonFormatter, keys, null));
			setHeaders(responseBuilder);
			return responseBuilder.build();
		}
		catch (NotAuthorizedException e)
		{
			logger.error("Attempt to access a local service.");
			return setHeaders(Response.status(Status.UNAUTHORIZED)).build();
		}
		catch (Exception e)
		{
			logger.error("Failed to get keys.", e);
			return setHeaders(Response.status(Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
		}
	}

	private Response getServiceValue(String service, String serviceKey, String key, boolean jsonResponse)
	{
		try
		{
			checkLocalService(service);
			ServiceKeyValue serviceKeyValue = m_keyStore.getValue(service, serviceKey, key);

			String value = null;

			if (serviceKeyValue != null)
			{
				value = serviceKeyValue.getValue();

				ResponseBuilder responseBuilder = Response.status(Response.Status.OK).entity(value);
				setHeaders(responseBuilder);
				return responseBuilder.build();
			}
			else
			{
				ResponseBuilder responseBuilder = setHeaders(Response.status(Status.NOT_FOUND));

				if (jsonResponse)
					responseBuilder.entity(new ErrorResponse("Key not found"));

				return responseBuilder.build();
			}


		}
		catch (NotAuthorizedException e)
		{
			logger.error("Attempt to access a local service.");
			return setHeaders(Response.status(Status.UNAUTHORIZED)).build();
		}
		catch (Exception e)
		{
			logger.error("Failed to retrieve value.", e);
			ResponseBuilder responseBuilder = setHeaders(Response.status(Status.INTERNAL_SERVER_ERROR));

			if (jsonResponse)
				responseBuilder.entity(new ErrorResponse(e.getMessage()));

			return responseBuilder.build();
		}
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/{service}/{serviceKey}/{key}")
	public Response getValue(@PathParam("service") String service, @PathParam("serviceKey")
			String serviceKey, @PathParam("key") String key)
	{
		return getServiceValue(service, serviceKey, key, true);
	}

	@GET
	@Produces(MediaType.TEXT_PLAIN + "; charset=UTF-8")
	@Path("/{service}/{serviceKey}/{key}")
	public Response getValuePlain(@PathParam("service") String service, @PathParam("serviceKey")
			String serviceKey, @PathParam("key") String key)
	{
		return getServiceValue(service, serviceKey, key, false);
	}

	@POST
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/{service}/{serviceKey}/{key}")
	public Response setValue(@PathParam("service") String service, @PathParam("serviceKey") String serviceKey,
			@PathParam("key") String key, String value)
	{
		try
		{
			checkLocalService(service);
			m_keyStore.setValue(service, serviceKey, key, value);
			return setHeaders(Response.status(Response.Status.NO_CONTENT)).build();
		}
		catch (NotAuthorizedException e)
		{
			logger.error("Attempt to access a local service.");
			return setHeaders(Response.status(Status.UNAUTHORIZED)).build();
		}
		catch (Exception e)
		{
			logger.error("Failed to add value.", e);
			return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
		}
	}

	@DELETE
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/{service}/{serviceKey}/{key}")
	public Response deleteKey(@PathParam("service") String service, @PathParam("serviceKey") String serviceKey,
			@PathParam("key") String key)
	{
		try
		{
			checkLocalService(service);
			m_keyStore.deleteKey(service, serviceKey, key);
			return setHeaders(Response.status(Response.Status.NO_CONTENT)).build();
		}
		catch (NotAuthorizedException e)
		{
			logger.error("Attempt to access a local service.");
			return setHeaders(Response.status(Status.UNAUTHORIZED)).build();
		}
		catch (Exception e)
		{
			logger.error("Failed to delete key.", e);
			return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
		}
	}

	private void checkLocalService(String service)
			throws NotAuthorizedException
	{
		if (service.startsWith("_"))
		{
			throw new NotAuthorizedException("Attempt to access an unauthorized service");
		}
	}

	private class NotAuthorizedException extends Exception
	{

		NotAuthorizedException(String message)
		{
			super(message);
		}
	}
}
