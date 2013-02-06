// OpenTSDB2
// Copyright (C) 2013 Proofpoint, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>
package net.opentsdb.core.http.rest.json;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class JsonResponseBuilder
{
	private List<String> errorMessages = new ArrayList<String>();
	private int status;

	public JsonResponseBuilder(Response.Status status)
	{
		checkNotNull(status);
		this.status = status.getStatusCode();
	}

	public JsonResponseBuilder addErrors(List<String> errorMessages)
	{
		this.errorMessages.addAll(errorMessages);
		return this;
	}

	public JsonResponseBuilder addError(String errorMessage)
	{
		errorMessages.add(errorMessage);
		return this;
	}

	public Response build()
	{
		ErrorResponse responseJson = new ErrorResponse(errorMessages);

		return Response
				.status(status)
				.type(MediaType.APPLICATION_JSON_TYPE)
				.entity(responseJson).build();
	}
}