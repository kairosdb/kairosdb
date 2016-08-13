/*
 * Copyright 2013 Proofpoint Inc.
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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

public class ErrorResponse
{
	private List<String> m_errors;

	@JsonCreator
	public ErrorResponse(@JsonProperty("errors") List<String> errors)
	{
		m_errors = errors;
	}

	public ErrorResponse(String error)
	{
		m_errors = Collections.singletonList(error);
	}

	@JsonProperty
	public List<String> getErrors()
	{
		return (m_errors);
	}
}
