package org.kairosdb.core.http.rest;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.kairosdb.core.http.rest.json.ErrorResponse;
import org.kairosdb.core.http.rest.json.JsonResponseBuilder;
import org.kairosdb.core.http.rest.json.QueryParser;
import org.kairosdb.core.http.rest.json.RollupResponse;
import org.kairosdb.rollup.RollUpException;
import org.kairosdb.rollup.RollUpTasksStore;
import org.kairosdb.rollup.RollupTask;
import org.kairosdb.rollup.RollupTaskStatus;
import org.kairosdb.rollup.RollupTaskStatusStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.kairosdb.core.http.rest.MetricsResource.setHeaders;
import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;

@Path("/api/v1/rollups")
public class RollUpResource
{
	private static final Logger logger = LoggerFactory.getLogger(MetricsResource.class);
	static final String RESOURCE_URL = "/api/v1/rollups/";

	private final QueryParser parser;
	private final RollUpTasksStore store;
	private final RollupTaskStatusStore statusStore;

	@Inject
	public RollUpResource(QueryParser parser, RollUpTasksStore store, RollupTaskStatusStore statusStore)
	{
		this.parser = checkNotNull(parser);
		this.store = checkNotNull(store);
		this.statusStore = checkNotNull(statusStore);
	}

	@POST
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	public Response create(String json)
	{
		checkNotNullOrEmpty(json);
		try
		{
			RollupTask task = parser.parseRollupTask(json);
			store.write(ImmutableList.of(task));
			ResponseBuilder responseBuilder = Response.status(Status.OK).entity(parser.getGson().toJson(createResponse(task)));
			setHeaders(responseBuilder);
			return responseBuilder.build();
		}
		catch (BeanValidationException e)
		{
			JsonResponseBuilder builder = new JsonResponseBuilder(Status.BAD_REQUEST);
			return builder.addErrors(e.getErrorMessages()).build();
		}
		catch (Exception e)
		{
			logger.error("Failed to add roll-up.", e);
			return setHeaders(Response.status(Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
		}
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	public Response list()
	{
		try
		{
			Map<String, RollupTask> tasks = store.read();

			StringBuilder json = new StringBuilder();
			json.append('[');
			for (RollupTask task : tasks.values())
			{
				json.append(task.getJson()).append(",");
			}

			if (json.length() > 1)
				json.deleteCharAt(json.length() - 1);
			json.append(']');

			ResponseBuilder responseBuilder = Response.status(Status.OK).entity(json.toString());
			setHeaders(responseBuilder);
			return responseBuilder.build();
		}
		catch (RollUpException e)
		{
			logger.error("Failed to list roll-ups.", e);
			return setHeaders(Response.status(Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
		}
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/status/{id}")
	public Response getStatus(@PathParam("id") String id)
	{
		checkNotNullOrEmpty(id);
		try
		{
			ResponseBuilder responseBuilder;
			RollupTaskStatus status = statusStore.read(id);
			if (status != null)
			{
				responseBuilder = Response.status(Status.OK).entity(parser.getGson().toJson(status));
			}
			else
			{
				responseBuilder = Response.status(Status.NOT_FOUND).entity(new ErrorResponse("Status not found for id " + id));
			}
			setHeaders(responseBuilder);
			return responseBuilder.build();
		}
		catch (RollUpException e)
		{
			logger.error("Failed to get status for roll-up.", e);
			return setHeaders(Response.status(Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
		}
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("{id}")
	public Response get(@PathParam("id") String id)
	{
		checkNotNullOrEmpty(id);
		try
		{
			ResponseBuilder responseBuilder;
			RollupTask task = store.read(id);
			if (task != null)
			{
				responseBuilder = Response.status(Status.OK).entity(task.getJson());
			}
			else
			{
				responseBuilder = Response.status(Status.NOT_FOUND).entity(new ErrorResponse("Resource not found for id " + id));
			}
			setHeaders(responseBuilder);
			return responseBuilder.build();
		}
		catch (RollUpException e)
		{
			logger.error("Failed to get roll-up.", e);
			return setHeaders(Response.status(Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
		}
	}

	@DELETE
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("{id}")
	public Response delete(@PathParam("id") String id)
	{
		try
		{
			checkNotNullOrEmpty(id);

			RollupTask task = store.read(id);
			if (task != null)
			{
				store.remove(id);
				return setHeaders(Response.status(Status.NO_CONTENT)).build();
			}
			else
			{
				ResponseBuilder responseBuilder = Response.status(Status.NOT_FOUND).entity(new ErrorResponse("Resource not found for id " + id));
				setHeaders(responseBuilder);
				return responseBuilder.build();
			}
		}
		catch (RollUpException e)
		{
			logger.error("Failed to delete roll-up.", e);
			return setHeaders(Response.status(Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
		}
	}

	@PUT
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("{id}")
	public Response update(@PathParam("id") String id, String json)
	{
		checkNotNullOrEmpty(id);
		checkNotNullOrEmpty(json);

		try
		{
			ResponseBuilder responseBuilder;
			RollupTask found = store.read(id);
			if (found == null)
			{
				responseBuilder = Response.status(Status.NOT_FOUND).entity(new ErrorResponse("Resource not found for id " + id));
			}
			else
			{
				RollupTask task = parser.parseRollupTask(json);
				RollupTask updatedTask = new RollupTask(id, task.getName(), task.getExecutionInterval(), task.getRollups(), task.getJson());
				store.write(ImmutableList.of(updatedTask));
				responseBuilder = Response.status(Status.OK).entity(parser.getGson().toJson(createResponse(updatedTask)));
			}
			setHeaders(responseBuilder);
			return responseBuilder.build();
		}
		catch (BeanValidationException e)
		{
			JsonResponseBuilder builder = new JsonResponseBuilder(Status.BAD_REQUEST);
			return builder.addErrors(e.getErrorMessages()).build();
		}
		catch (Exception e)
		{
			logger.error("Failed to add roll-up.", e);
			return setHeaders(Response.status(Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
		}
	}

	private RollupResponse createResponse(RollupTask task)
	{
		return new RollupResponse(task.getId(), task.getName(), RESOURCE_URL + task.getId());
	}
}
