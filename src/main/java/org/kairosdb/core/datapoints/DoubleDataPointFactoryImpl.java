package org.kairosdb.core.datapoints;

import org.kairosdb.core.DataPoint;

import java.nio.ByteBuffer;

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
