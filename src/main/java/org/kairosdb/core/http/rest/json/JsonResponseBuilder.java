/*
 * Copyright 2016 KairosDB Authors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.kairosdb.core.http.rest.json;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class JsonResponseBuilder
{
	private List<String> errorMessages = new ArrayList<String>();
	private int status;

	public JsonResponseBuilder(Response.Status status)
	{
		requireNonNull(status);
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
				.header("Access-Control-Allow-Origin", "*")
				.type(MediaType.APPLICATION_JSON_TYPE)
				.entity(responseJson).build();
	}
}