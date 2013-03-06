//
//  DataPointRequest.java
//
// Copyright 2013, Proofpoint Inc. All rights reserved.
//        
package org.kairosdb.core.http.rest.json;

import org.apache.bval.constraints.NotEmpty;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

public class DataPointRequest
{
	@NotNull
	@NotEmpty
	private String value;

	@Min(1)
	private long timestamp;

	public DataPointRequest(long timestamp, String value)
	{
		this.timestamp = timestamp;
		this.value = value;
	}

	public String getValue()
	{
		return value;
	}

	public long getTimestamp()
	{
		return timestamp;
	}
}