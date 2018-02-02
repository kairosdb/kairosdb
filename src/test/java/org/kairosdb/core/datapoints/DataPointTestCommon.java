package org.kairosdb.core.datapoints;

import org.junit.Test;
import org.kairosdb.core.DataPoint;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;

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
	public void testBufferSerialization() throws IOException
	{
		ByteArrayOutputStream buffer = new ByteArrayOutputStream(1024);

		DataOutputStream dataOutputStream = new DataOutputStream(buffer);
		for (DataPoint dataPoint : dataPointList)
		{
			dataPoint.writeValueToBuffer(dataOutputStream);
		}

		double testSum = 0.0;

		DataInputStream dataInputStream = new DataInputStream(
				new ByteArrayInputStream(buffer.toByteArray()));

		for (int i = 0; i < dataPointList.size(); i++)
		{
			DataPoint dp = factory.getDataPoint(dataPointList.get(i).getTimestamp(),
					dataInputStream);

			assertEquals(dataPointList.get(i), dp);
			testSum += dp.getDoubleValue();
		}

		assertEquals(sum, testSum, 0.0001);
	}

	@Test
	public void testEqualsHashCode() {
		final HashSet<DataPoint> dataPointsSet = new HashSet<>(dataPointList);

		assertEquals(dataPointList.size(), dataPointsSet.size());
	}
}
