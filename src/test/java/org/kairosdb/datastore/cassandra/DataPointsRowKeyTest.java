package org.kairosdb.datastore.cassandra;

import org.junit.Test;

public class DataPointsRowKeyTest
{

	@Test(expected = NullPointerException.class)
	public void test_constructor_null_metricName_invalid()
	{
		new DataPointsRowKey(null, "foo", 123L, "datatype");
	}

	@Test(expected = IllegalArgumentException.class)
	public void test_constructor_empty_metricName_invalid()
	{
		new DataPointsRowKey("", "foo", 123L, "datatype");
	}

	@Test(expected = NullPointerException.class)
	public void test_constructor_null_dataType_invalid()
	{
		new DataPointsRowKey("myMetric", "foo", 123L, null);
	}
}