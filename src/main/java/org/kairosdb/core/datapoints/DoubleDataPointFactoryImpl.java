package org.kairosdb.core.datapoints;

import com.google.gson.JsonElement;
import org.kairosdb.core.DataPoint;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

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
	public DataPoint getDataPoint(long timestamp, JsonElement json)
	{
		double value = 0.0;
		if (!json.isJsonNull())
			value = json.getAsDouble();
		return new DoubleDataPoint(timestamp, value);
	}

	@Override
	public DataPoint getDataPoint(long timestamp, DataInput buffer) throws IOException
	{
		double value = buffer.readDouble();

		return new DoubleDataPoint(timestamp, value);
	}

	/*public static ByteBuffer writeToByteBuffer(DoubleDataPoint dataPoint)
	{
		ByteBuffer buffer = ByteBuffer.allocate(8);

		writeToByteBuffer(buffer, dataPoint);

		buffer.flip();
		return (buffer);
	}*/

	public static void writeToByteBuffer(DataOutput buffer, DoubleDataPoint dataPoint) throws IOException
	{
		buffer.writeDouble(dataPoint.getDoubleValue());
	}
}
