package org.kairosdb.core.http.rest;

import org.kairosdb.core.http.rest.json.GsonParser;
import org.kairosdb.core.http.rest.json.JsonResponseBuilder;
import org.kairosdb.core.http.rest.json.ValidationErrors;
import org.kairosdb.rollup.RollUpTask;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static com.google.common.base.Preconditions.checkNotNull;

@Path("/api/v1/rollups")
public class RollUpResource
{
	private final GsonParser parser;

	public RollUpResource(GsonParser parser)
	{
		this.parser = checkNotNull(parser);
	}

	@POST
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/rollup")
	public Response create(String json)
	{
		ValidationErrors validationErrors = null;

		RollUpTask task = parser.parseRollUpTask(json);

		JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
		for (String errorMessage : validationErrors.getErrors())
		{
			builder.addError(errorMessage);
		}
		return builder.build();
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("rollup")
	public Response list()
	{
		ValidationErrors validationErrors = null;

		JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
		for (String errorMessage : validationErrors.getErrors())
		{
			builder.addError(errorMessage);
		}
		return builder.build();
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("rollup/{id}")
	public Response get(@PathParam("id") String id)
	{
		ValidationErrors validationErrors = null;

		JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
		for (String errorMessage : validationErrors.getErrors())
		{
			builder.addError(errorMessage);
		}
		return builder.build();
	}

	@DELETE
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("delete/{id}")
	public Response delete(@PathParam("id") String id) throws Exception
	{
		return setHeaders(Response.status(Response.Status.NO_CONTENT)).build();
	}

	@PUT
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("update/{id}")
	public Response update(@PathParam("id") String id, String json)
	{
		return setHeaders(Response.status(Response.Status.NO_CONTENT)).build();
	}

	// todo also used in Metrics Resource.Should this be defined in a common place
	private Response.ResponseBuilder setHeaders(Response.ResponseBuilder responseBuilder)
	{
		responseBuilder.header("Access-Control-Allow-Origin", "*");
		responseBuilder.header("Pragma", "no-cache");
		responseBuilder.header("Cache-Control", "no-cache");
		responseBuilder.header("Expires", 0);

		return (responseBuilder);
	}
}
