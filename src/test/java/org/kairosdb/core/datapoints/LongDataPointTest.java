package org.kairosdb.core.datapoints;

import org.junit.BeforeClass;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 12/9/13
 Time: 2:27 PM
 To change this template use File | Settings | File Templates.
 */
public class LongDataPointTest extends DataPointTestCommon
{
	@BeforeClass
	public static void setup()
	{
		LongDataPointFactory longFactory = new LongDataPointFactoryImpl();
		factory = longFactory;

		dataPointList.clear();
		dataPointList.add(longFactory.createDataPoint(1, 1));
		dataPointList.add(longFactory.createDataPoint(1, 123));
		dataPointList.add(longFactory.createDataPoint(123, 1));
		dataPointList.add(longFactory.createDataPoint(123, 123));
		dataPointList.add(longFactory.createDataPoint(1234, 1234));
		dataPointList.add(longFactory.createDataPoint(65537, 65537));
		dataPointList.add(longFactory.createDataPoint(4294967296L, 4294967296L));
		dataPointList.add(longFactory.createDataPoint(1234567890L, 1234567890L));

		sum = 5529602205.0;
	}
}
