package org.kairosdb.core.datapoints;

import org.junit.BeforeClass;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 12/9/13
 Time: 2:56 PM
 To change this template use File | Settings | File Templates.
 */
public class DoubleDataPointTest extends DataPointTestCommon
{
	@BeforeClass
	public static void setup()
	{
		DoubleDataPointFactory doubleFactory = new DoubleDataPointFactoryImpl();
		factory = doubleFactory;

		dataPointList.clear();
		dataPointList.add(doubleFactory.createDataPoint(123, 123.0));
		dataPointList.add(doubleFactory.createDataPoint(1, 12345.67890));
		dataPointList.add(doubleFactory.createDataPoint(1, 1.0));
		dataPointList.add(doubleFactory.createDataPoint(123, 1.0));

		sum = 12470.6789;
	}
}
