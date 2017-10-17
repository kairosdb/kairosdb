package org.kairosdb.core.http.exceptionmapper;

import org.kairosdb.core.exception.InvalidServerTypeException;

import javax.inject.Singleton;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
@Singleton
public class InvalidServerTypeExceptionMapper implements ExceptionMapper<InvalidServerTypeException>
{
	@Override
	public Response toResponse(InvalidServerTypeException e)
	{
		return Response.status(Response.Status.FORBIDDEN)
				.type(MediaType.APPLICATION_JSON_TYPE)
				.entity(e.getMessage())
				.build();
	}
}
