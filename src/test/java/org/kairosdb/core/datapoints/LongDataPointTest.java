package org.kairosdb.core.datapoints;

import org.junit.Before;
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

		dataPointList.add(longFactory.createDataPoint(123, 1));
		dataPointList.add(longFactory.createDataPoint(123, 123));
		dataPointList.add(longFactory.createDataPoint(123, 1234));
		dataPointList.add(longFactory.createDataPoint(123, 65537));
		dataPointList.add(longFactory.createDataPoint(123, 4294967296L));
		dataPointList.add(longFactory.createDataPoint(1, 1234567890L));

		sum = 5529602081.0;
	}
}
