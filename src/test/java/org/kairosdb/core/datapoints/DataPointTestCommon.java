package org.kairosdb.core.datapoints;

import org.junit.Test;
import org.kairosdb.core.DataPoint;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.assertEquals;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 12/9/13
 Time: 2:27 PM
 To change this template use File | Settings | File Templates.
 */
public class DataPointTestCommon
{
	public static DataPointFactory factory;
	public static List<DataPoint> dataPointList = new ArrayList<DataPoint>();
	public static double sum = 0.0;

	@Test
	public void testBufferSerialization()
	{
		ByteBuffer buffer = ByteBuffer.allocate(1024);

		for (DataPoint dataPoint : dataPointList)
		{
			dataPoint.writeValueToBuffer(buffer);
		}

		double testSum = 0.0;
		buffer.flip();
		for (int i = 0; i < dataPointList.size(); i++)
		{
			DataPoint dp = factory.getDataPoint(dataPointList.get(i).getTimestamp(),
					buffer);

			assertEquals(dataPointList.get(i), dp);
			testSum += dp.getDoubleValue();
		}

		assertEquals(sum, testSum);
	}
}
