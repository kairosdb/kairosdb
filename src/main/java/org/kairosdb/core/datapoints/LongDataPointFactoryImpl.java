package org.kairosdb.core.datapoints;

import org.kairosdb.core.DataPoint;

import java.nio.ByteBuffer;

import static org.kairosdb.core.DataPoint.API_LONG;
import static org.kairosdb.core.DataPoint.GROUP_NUMBER;

public class LongDataPointFactoryImpl implements LongDataPointFactory
{
	public static final String DST_LONG = "kairos_long";

	public static LongDataPoint getFromByteBuffer(ByteBuffer buffer)
	{
		return null;
	}

	public static void writeToByteBuffer(ByteBuffer buffer, LongDataPoint dataPoint)
	{
		boolean writeRest = false;

		long value = dataPoint.getValue();
		if (value != 0L)  //Short circuit for zero values
		{
			for (int I = 1; I <= 8; I++)
			{
				byte b = (byte)((value >>> (64 - (8 * I))) & 0xFF);
				if (writeRest || b != 0)
				{
					buffer.put(b);
					writeRest = true;
				}
			}
		}
	}

	public static ByteBuffer writeToByteBuffer(LongDataPoint dataPoint)
	{
		ByteBuffer buffer = ByteBuffer.allocate(8);

		writeToByteBuffer(buffer, dataPoint);

		buffer.flip();
		return (buffer);
	}

	@Override
	public DataPoint createDataPoint(long timestamp, long value)
	{
		return ((DataPoint)new LongDataPoint(timestamp, value));
	}

	@Override
	public String getDataStoreType()
	{
		return DST_LONG;
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
}
