package org.kairosdb.core.datapoints;

import com.google.gson.JsonElement;
import org.kairosdb.core.DataPoint;

import java.io.DataInput;
import java.io.IOException;

import static org.kairosdb.util.Util.unpackLong;

public class LongNanoDataPointFactoryImpl extends LongDataPointFactoryImpl
{
	public static final String DST_LONG_NANO = "kairos_long_nano";

	public static LongDataPoint getFromByteBuffer(long timestamp, DataInput buffer) throws IOException
	{
		long value = unpackLong(buffer);

		return new LongNanoDataPoint(timestamp, value);
	}

	@Override
	public DataPoint createDataPoint(long timestamp, long value)
	{
		return ((DataPoint)new LongNanoDataPoint(timestamp, value));
	}

	@Override
	public DataPoint getDataPoint(long timestamp, JsonElement json)
	{
		long value = 0L;
		if (!json.isJsonNull())
			value = json.getAsLong();
		return new LongNanoDataPoint(timestamp, value);
	}

	@Override
	public String getDataStoreType()
	{
		return DST_LONG_NANO;
	}
}
