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