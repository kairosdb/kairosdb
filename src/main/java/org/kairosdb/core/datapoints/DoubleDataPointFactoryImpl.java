package org.kairosdb.core.datapoints;

import org.kairosdb.core.DataPoint;

import java.nio.ByteBuffer;

import static org.kairosdb.core.DataPoint.API_DOUBLE;
import static org.kairosdb.core.DataPoint.GROUP_NUMBER;

public class DoubleDataPointFactoryImpl implements DoubleDataPointFactory
{
	public static final String DST_DOUBLE = "kairos_double";

	@Override
	public DataPoint createDataPoint(long timestamp, double value)
	{
		return ((DataPoint)new DoubleDataPoint(timestamp, value));
	}

	@Override
	public String getDataStoreType()
	{
		return DST_DOUBLE;
	}

	@Override
	public String getGroupType()
	{
		return GROUP_NUMBER;
	}

	@Override
	public DataPoint getDataPoint(long timestamp, String json)
	{
		return null;  //To change body of implemented methods use File | Settings | File Templates.
	}

	@Override
	public DataPoint getDataPoint(long timestamp, ByteBuffer buffer)
	{
		return null;  //To change body of implemented methods use File | Settings | File Templates.
	}

	public static ByteBuffer writeToByteBuffer(DoubleDataPoint dataPoint)
	{
		ByteBuffer buffer = ByteBuffer.allocate(8);

		writeToByteBuffer(buffer, dataPoint);

		buffer.flip();
		return (buffer);
	}

	public static void writeToByteBuffer(ByteBuffer buffer, DoubleDataPoint dataPoint)
	{
		buffer.putDouble(dataPoint.getDoubleValue());
	}
}
