//
//  ValidationException.java
//
// Copyright 2013, Proofpoint Inc. All rights reserved.
//        
package org.kairosdb.core.http.rest.validation;

public class ValidationException extends Exception

{
	public ValidationException(String message)
	{
		super(message);
	}
}