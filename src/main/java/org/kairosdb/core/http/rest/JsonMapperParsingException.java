// KairosDB2
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
package org.kairosdb.core.http.rest;

import java.io.IOException;

/**
 * Wraps JsonProcessingExceptions to provide more information about parsing errors.
 */
public class JsonMapperParsingException extends IOException
{
	private Class<?> type;

	public JsonMapperParsingException(Class<?> type, Throwable cause)
	{
		super(String.format("Invalid json for Java type %s", type.getSimpleName()), cause);

		this.type = type;
	}

	/**
	 * Returns the type of object that failed Json parsing.
	 *
	 * @return object type of object that failed Json parsing
	 */
	public Class<?> getType()
	{
		return type;
	}
}