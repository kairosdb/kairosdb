package org.kairosdb.core.datapoints;

import org.json.JSONException;
import org.json.JSONWriter;
import org.kairosdb.core.DataPoint;

import java.nio.ByteBuffer;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 12/9/13
 Time: 1:25 PM
 To change this template use File | Settings | File Templates.
 */
public class LegacyDoubleDataPoint extends LegacyDataPoint
{
	private double m_value;

	public LegacyDoubleDataPoint(long timestamp, double value)
	{
		super(timestamp);
		m_value = value;
	}

	@Override
	public ByteBuffer toByteBuffer()
	{
		return LegacyDataPointFactory.writeToByteBuffer(this);
	}

	@Override
	public void writeValueToBuffer(ByteBuffer buffer)
	{
		LegacyDataPointFactory.writeToByteBuffer(buffer, this);
	}

	@Override
	public void writeValueToJson(JSONWriter writer) throws JSONException
	{

	}

	@Override
	public String getApiDataType()
	{
		return DataPoint.API_DOUBLE;
	}

	@Override
	public boolean isDouble()
	{
		return true;
	}

	@Override
	public double getDoubleValue()
	{
		return m_value;
	}
}
