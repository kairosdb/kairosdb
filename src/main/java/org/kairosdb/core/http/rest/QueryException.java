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

public class QueryException extends Exception
{
	/**
	 Constructs a new {@code QueryException} with {@code null} as its detail
	 message.
	 */
	public QueryException()
	{
	}

	/**
	 Constructs a new {@code QueryException} with the specified detail message.

	 @param message the detail message
	 */
	public QueryException(String message)
	{
		super(message);
	}

	/**
	 Constructs a new {@code QueryException} with the specified cause and a detail message of
	 {@code (cause == null ? null : cause.toString())} (which typically contains the class
	 and detail message of {@code cause}). This constructor is useful for exceptions that are
	 little more than wrappers for other throwables.

	 @param cause the cause (an exception to be wrapped); {@code null} = the cause is
	 nonexistent or unknown
	 */
	public QueryException(Throwable cause)
	{
		super(cause);
	}

	/**
	 Constructs a new {@code QueryException} with the specified detail message and cause.
	 <p>
	 Note that the detail message associated with {@code cause} is <i>not</i> automatically
	 incorporated in this exception's detail message.

	 @param message the detail message
	 @param cause   the cause (an exception to be wrapped); {@code null} = the cause is
	 nonexistent or unknown
	 */
	public QueryException(String message, Throwable cause)
	{
		super(message, cause);
	}
}
