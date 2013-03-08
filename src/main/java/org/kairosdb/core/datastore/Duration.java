//
// Duration.java
//
// Copyright 2013, NextPage Inc. All rights reserved.
//

package org.kairosdb.core.datastore;


import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

public class Duration
{
	@Min(1)
	protected int value;

	@NotNull
	protected TimeUnit unit;

	public Duration()
	{
	}

	public Duration(int value, TimeUnit unit)
	{
		this.value = value;
		this.unit = unit;
	}

	public int getValue()
	{
		return value;
	}

	public TimeUnit getUnit()
	{
		return unit;
	}

	@Override
	public String toString()
	{
		return "Duration{" +
				"value=" + value +
				", unit=" + unit +
				'}';
	}
}
