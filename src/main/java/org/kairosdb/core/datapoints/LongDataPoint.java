package org.kairosdb.core.datapoints;


import org.json.JSONException;
import org.json.JSONWriter;

import java.nio.ByteBuffer;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 8/31/13
 Time: 7:22 AM
 To change this template use File | Settings | File Templates.
 */
public class LongDataPoint extends DataPointHelper
{
	private long m_value;

	public LongDataPoint(long timestamp, long value)
	{
		super(timestamp);
		m_value = value;
	}

	public long getValue()
	{
		return (m_value);
	}

	@Override
	public double getDoubleValue()
	{
		return (double)m_value;
	}

	@Override
	public ByteBuffer toByteBuffer()
	{
		return (LongDataPointFactoryImpl.writeToByteBuffer(this));
	}

	@Override
	public void writeToBuffer(ByteBuffer buffer)
	{
		LongDataPointFactoryImpl.writeToByteBuffer(buffer, this);
	}

	@Override
	public void writeJson(JSONWriter writer) throws JSONException
	{
		writer.value(m_value);
	}

	@Override
	public String getApiDataType()
	{
		return API_LONG;
	}

	@Override
	public String getDataStoreDataType()
	{
		return LongDataPointFactoryImpl.DST_LONG;
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
	public boolean isDouble()
	{
		return true;
	}

}
