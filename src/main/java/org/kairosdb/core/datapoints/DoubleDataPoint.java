package org.kairosdb.core.datapoints;


import org.json.JSONException;
import org.json.JSONWriter;

import java.nio.ByteBuffer;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 8/31/13
 Time: 7:20 AM
 To change this template use File | Settings | File Templates.
 */
public class DoubleDataPoint extends DataPointHelper
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
		return DoubleDataPointFactoryImpl.writeToByteBuffer(this);
	}

	@Override
	public void writeValueToBuffer(ByteBuffer buffer)
	{
		DoubleDataPointFactoryImpl.writeToByteBuffer(buffer, this);
	}

	@Override
	public void writeJson(JSONWriter writer) throws JSONException
	{
		if (m_value != m_value || Double.isInfinite(m_value))
			throw new IllegalStateException("NaN or Infinity:" + m_value + " data point=" + this);

		writer.value(m_value);
	}

	@Override
	public String getApiDataType()
	{
		return API_DOUBLE;
	}

	@Override
	public String getDataStoreDataType()
	{
		return DoubleDataPointFactoryImpl.DST_DOUBLE;
	}

	@Override
	public boolean isLong()
	{
		return false;
	}

	@Override
	public long getLongValue()
	{
		return (long)m_value;
	}

	@Override
	public boolean isDouble()
	{
		return true;
	}

}
