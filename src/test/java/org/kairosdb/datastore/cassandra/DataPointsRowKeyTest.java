package org.kairosdb.datastore.cassandra;

import org.junit.Test;

public class DataPointsRowKeyTest
{

	@Test(expected = NullPointerException.class)
	public void test_constructor_null_metricName_invalid()
	{
		new DataPointsRowKey(null, 123L, "datatype");
	}

	@Test(expected = IllegalArgumentException.class)
	public void test_constructor_empty_metricName_invalid()
	{
		new DataPointsRowKey("", 123L, "datatype");
	}

	@Test(expected = NullPointerException.class)
	public void test_constructor_null_dataType_invalid()
	{
		new DataPointsRowKey("myMetric", 123L, null);
	}
}