package org.kairosdb.core.datapoints;

import org.kairosdb.core.NumericDataPoint;

import java.nio.ByteBuffer;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 8/31/13
 Time: 7:20 AM
 To change this template use File | Settings | File Templates.
 */
public class DoubleDataPoint extends DataPointHelper implements NumericDataPoint
{
	private double m_value;

	public DoubleDataPoint(long timestamp, double value)
	{
		super(timestamp);
		m_value = value;
	}

	@Override
	public double getDoubleValue()
	{
		return (m_value);
	}

	@Override
	public ByteBuffer toByteBuffer()
	{
		return null;  //To change body of implemented methods use File | Settings | File Templates.
	}

	@Override
	public String getApiDataType()
	{
		return API_DOUBLE;
	}

	@Override
	public String getDataStoreDataType()
	{
		return "double";
	}
}
