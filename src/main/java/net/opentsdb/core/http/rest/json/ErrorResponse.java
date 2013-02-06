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

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

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
