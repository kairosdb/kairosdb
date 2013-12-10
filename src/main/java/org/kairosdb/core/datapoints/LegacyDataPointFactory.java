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

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 12/9/13
 Time: 12:48 PM
 To change this template use File | Settings | File Templates.
 */
public class LegacyDataPointFactory implements DataPointFactory
{
	public static final int LONG_VALUE = 0;
	public static final int DOUBLE_VALUE = 1;

	public static final String DATASTORE_TYPE = "kairos_legacy";
	public static final String API_TYPE = "legacy";

	/*public static ByteBuffer writeToByteBuffer(LegacyLongDataPoint dataPoint)
	{
		ByteBuffer buffer = ByteBuffer.allocate(10);

		writeToByteBuffer(buffer, dataPoint);

		buffer.flip();
		return (buffer);
	}*/

	public static void writeToByteBuffer(DataOutput buffer, LegacyLongDataPoint dataPoint) throws IOException
	{
		long value = dataPoint.getLongValue();
		buffer.writeByte(LONG_VALUE);
		packLong(value, buffer);
	}

	/*public static ByteBuffer writeToByteBuffer(LegacyDoubleDataPoint dataPoint)
	{
		ByteBuffer buffer = ByteBuffer.allocate(9);

		writeToByteBuffer(buffer, dataPoint);

		buffer.flip();
		return (buffer);
	}*/

	public static void writeToByteBuffer(DataOutput buffer, LegacyDoubleDataPoint dataPoint) throws IOException
	{
		buffer.writeByte(DOUBLE_VALUE);
		buffer.writeDouble(dataPoint.getDoubleValue());
	}

	@Override
	public String getDataStoreType()
	{
		return DATASTORE_TYPE;
	}

	@Override
	public String getGroupType()
	{
		return GROUP_NUMBER;
	}

	@Override
	public DataPoint getDataPoint(long timestamp, JsonElement json)
	{
		return null;  //Should never be called for this factory
	}

	@Override
	public DataPoint getDataPoint(long timestamp, DataInput buffer) throws IOException
	{
		DataPoint ret;

		int type = buffer.readByte();
		if (type == LONG_VALUE)
		{
			ret = new LegacyLongDataPoint(timestamp, unpackLong(buffer));
		}
		else
		{
			ret = new LegacyDoubleDataPoint(timestamp, buffer.readDouble());
		}

		return ret;
	}
}
