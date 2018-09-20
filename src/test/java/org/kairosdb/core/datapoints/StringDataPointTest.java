package org.kairosdb.core.datapoints;

import org.junit.BeforeClass;

public class StringDataPointTest extends DataPointTestCommon
{
	@BeforeClass
	public static void setup()
	{
		StringDataPointFactory dpFactory = new StringDataPointFactory();
		factory = dpFactory;

		dataPointList.clear();
		dataPointList.add(dpFactory.createDataPoint(1, "Bob"));
		dataPointList.add(dpFactory.createDataPoint(1, "Bob Dog"));
		dataPointList.add(dpFactory.createDataPoint(123, "fo.com"));
		dataPointList.add(dpFactory.createDataPoint(123, "123"));
		dataPointList.add(dpFactory.createDataPoint(1234, "1.2.3.4"));

		sum = 0;
	}
}
