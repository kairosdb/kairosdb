package org.kairosdb.core.http.exceptionmapper;

import org.kairosdb.core.exception.InvalidServerTypeException;

import jakarta.inject.Singleton;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

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
