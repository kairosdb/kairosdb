package org.kairosdb.core.datapoints;

import com.google.gson.JsonElement;
import org.kairosdb.core.DataPoint;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.kairosdb.core.DataPoint.GROUP_NUMBER;
import static org.kairosdb.util.Util.packLong;
import static org.kairosdb.util.Util.unpackLong;

public class LongDataPointFactoryImpl implements LongDataPointFactory
{
	public static final String DST_LONG = "kairos_long";


	public static LongDataPoint getFromByteBuffer(long timestamp, DataInput buffer) throws IOException
	{
		long value = unpackLong(buffer);

		return new LongDataPoint(timestamp, value);
	}

	public static void writeToByteBuffer(DataOutput buffer, LongDataPoint dataPoint) throws IOException
	{
		long value = dataPoint.getValue();
		packLong(value, buffer);
	}

	public static ByteBuffer writeToByteBuffer(LongDataPoint dataPoint)
	{
		ByteBuffer buffer = ByteBuffer.allocate(9);

		//writeToByteBuffer(buffer, dataPoint);

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
	public DataPoint getDataPoint(long timestamp, JsonElement json)
	{
		long value = 0L;
		if (!json.isJsonNull())
			value = json.getAsLong();
		return new LongDataPoint(timestamp, value);
	}

	@Override
	public DataPoint getDataPoint(long timestamp, DataInput buffer) throws IOException
	{
		return getFromByteBuffer(timestamp, buffer);
	}
}
