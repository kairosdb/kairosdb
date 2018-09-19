package org.kairosdb.core.datapoints;

import org.junit.BeforeClass;

public class SnappyStringDataPointTest extends DataPointTestCommon
{
	@BeforeClass
	public static void setup()
	{
		SnappyStringDataPointFactory dpFactory = new SnappyStringDataPointFactory();
		factory = dpFactory;

		dataPointList.clear();
		dataPointList.add(dpFactory.createDataPoint(1, "Bob"));
		dataPointList.add(dpFactory.createDataPoint(1, "Bob Dog"));
		dataPointList.add(dpFactory.createDataPoint(123, "fo.com"));
		dataPointList.add(dpFactory.createDataPoint(123, "123"));
		dataPointList.add(dpFactory.createDataPoint(1234, "1.2.3.4 This is a long string that will get compressed by snappy :)"));
	}
}
