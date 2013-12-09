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
public class LegacyLongDataPoint extends LegacyDataPoint
{
	private long m_value;

	public LegacyLongDataPoint(long timestamp, long l)
	{
		super(timestamp);
		m_value = l;
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
		//To change body of implemented methods use File | Settings | File Templates.
	}

	@Override
	public String getApiDataType()
	{
		return DataPoint.API_LONG;
	}

	@Override
	public boolean isLong()
	{
		return true;
	}

	@Override
	public long getLongValue()
	{
		return m_value;
	}

	@Override
	public double getDoubleValue()
	{
		return (double)m_value;
	}
}
