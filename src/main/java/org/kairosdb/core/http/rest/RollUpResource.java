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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;

@Path("/api/v1/rollups")
public class RollUpResource
{
	private static final Logger logger = LoggerFactory.getLogger(MetricsResource.class);
	public static final String RESOURCE_URL = "/api/v1/rollups/";

	private final QueryParser parser;
	private final RollUpTasksStore store;

	@Inject
	public RollUpResource(QueryParser parser, RollUpTasksStore store)
	{
		this.parser = checkNotNull(parser);
		this.store = checkNotNull(store);
	}

	//	@POST
	//	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	//	@Path("/{id}/queries")
	//	public Response addQuery(@PathParam("id") String id, String json)
	//	{
	//		checkNotNullOrEmpty(id);
	//		checkNotNullOrEmpty(json);
	//		try
	//		{
	//			ResponseBuilder responseBuilder;
	//			RollupTask task = findExistingTask(id);
	//			if (task != null)
	//			{
	//				List<QueryMetric> queryMetrics = parser.parseQueryMetric(json);
	//				Rollup rollup = new Rollup();
	//				rollup.addQuery(queryMetrics.get(0));
	//				task.addRollup(rollup);
	//				store.write(ImmutableList.of(task));
	//				responseBuilder = Response.status(Status.NO_CONTENT);
	//			}
	//			else
	//			{
	//				responseBuilder = Response.status(Status.NOT_FOUND).entity(new ErrorResponse("Resource not found for id " + id));
	//			}
	//			setHeaders(responseBuilder);
	//			return responseBuilder.build();
	//		}
	//		catch (Exception e)
	//		{
	//			logger.error("Failed to create roll-up.", e);
	//			return setHeaders(Response.status(Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
	//		}
	//	}
	//
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
			List<RollupTask> tasks = store.read();

			StringBuilder json = new StringBuilder();
			json.append('[');
			for (RollupTask task : tasks)
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
	@Path("{id}")
	public Response get(@PathParam("id") String id)
	{
		checkNotNullOrEmpty(id);
		try
		{
			ResponseBuilder responseBuilder;
			RollupTask found = null;
			List<RollupTask> tasks = store.read();
			for (RollupTask task : tasks)
			{
				if (task.getId().equals(id))
				{
					found = task;
					break;
				}
			}

			if (found != null)
			{
				responseBuilder = Response.status(Status.OK).entity(found.getJson());
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

			if (findExistingTask(id) != null)
			{
				store.remove(id);
				return setHeaders(Response.status(Status.NO_CONTENT)).entity("").build();
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
			if (findExistingTask(id) == null)
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

	private RollupTask findExistingTask(String id) throws RollUpException
	{
		List<RollupTask> tasks = store.read();
		for (RollupTask task : tasks)
		{
			if (task.getId().equals(id))
			{
				return task;
			}
		}

		return null;
	}

	private ResponseBuilder setHeaders(ResponseBuilder responseBuilder)
	{
		responseBuilder.header("Access-Control-Allow-Origin", "*");
		responseBuilder.header("Pragma", "no-cache");
		responseBuilder.header("Cache-Control", "no-cache");
		responseBuilder.header("Expires", 0);

		return (responseBuilder);
	}
}
