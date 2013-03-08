//
// QueryException.java
//
// Copyright 2013, NextPage Inc. All rights reserved.
//

package org.kairosdb.core.http.rest;

/**
 Thrown when [TODO: describe when this exception is thrown]...
 */
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
	 <p/>
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
