package org.kairosdb.core.datapoints;

import org.kairosdb.core.DataPoint;

import java.nio.ByteBuffer;

import static org.kairosdb.core.DataPoint.API_LONG;
import static org.kairosdb.core.DataPoint.GROUP_NUMBER;
import static org.kairosdb.util.Util.packLong;
import static org.kairosdb.util.Util.unpackLong;

public class LongDataPointFactoryImpl implements LongDataPointFactory
{
	public static final String DST_LONG = "kairos_long";


	public static LongDataPoint getFromByteBuffer(long timestamp, ByteBuffer buffer)
	{
		long value = unpackLong(buffer);

		return new LongDataPoint(timestamp, value);
	}

	public static void writeToByteBuffer(ByteBuffer buffer, LongDataPoint dataPoint)
	{
		long value = dataPoint.getValue();
		packLong(value, buffer);
	}

	public static ByteBuffer writeToByteBuffer(LongDataPoint dataPoint)
	{
		ByteBuffer buffer = ByteBuffer.allocate(9);

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
		return getFromByteBuffer(timestamp, buffer);
	}
}
