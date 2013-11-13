//
//  DataPointTest.java
//
// Copyright 2013, Proofpoint Inc. All rights reserved.
//        
package org.kairosdb.core;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class DataPointTest
{
	@Test(expected = NullPointerException.class)
	public void test_constructorStringValue_nullValue_invalid()
	{
		new DataPoint(1234L, null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void test_constructorStringValue_emptyValue_invalid()
	{
		new DataPoint(1234L, "");
	}

	@Test
	public void constructorStringValue_long()
	{
		DataPoint dataPoint = new DataPoint(1234L, "40");

		assertThat(dataPoint.isInteger(), equalTo(true));
		assertThat(dataPoint.getTimestamp(), equalTo(1234L));
		assertThat(dataPoint.getLongValue(), equalTo(40L));
	}

	@Test
	public void constructorStringValue_double()
	{
		DataPoint dataPoint = new DataPoint(1234L, "40.3");

		assertThat(dataPoint.isInteger(), equalTo(false));
		assertThat(dataPoint.getTimestamp(), equalTo(1234L));
		assertThat(dataPoint.getDoubleValue(), equalTo(40.3));
	}

	@Test(expected = NumberFormatException.class)
	public void constructorStringValue_value_invalid()
	{
		DataPoint dataPoint = new DataPoint(1234L, "40.a");

		assertThat(dataPoint.isInteger(), equalTo(false));
		assertThat(dataPoint.getTimestamp(), equalTo(1234L));
		assertThat(dataPoint.getDoubleValue(), equalTo(40.3));
	}
}