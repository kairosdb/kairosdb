package org.kairosdb.core.datapoints;

import org.kairosdb.core.DataPoint;

import java.nio.ByteBuffer;

public class LongDataPointFactoryImpl implements LongDataPointFactory
{
	public static final String DST_LONG = "kairos_long";

	public static LongDataPoint getFromByteBuffer(ByteBuffer buffer)
	{
		return null;
	}

	public static ByteBuffer writeToByteBuffer(LongDataPoint dataPoint)
	{
		return null;
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
