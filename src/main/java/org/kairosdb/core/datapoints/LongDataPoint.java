package org.kairosdb.core.datapoints;


import org.json.JSONException;
import org.json.JSONWriter;

import java.io.DataOutput;
import java.io.IOException;
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

	/*@Override
	public ByteBuffer toByteBuffer()
	{
		return (LongDataPointFactoryImpl.writeToByteBuffer(this));
	}*/

	@Override
	public void writeValueToBuffer(DataOutput buffer) throws IOException
	{
		LongDataPointFactoryImpl.writeToByteBuffer(buffer, this);
	}

	@Override
	public void writeValueToJson(JSONWriter writer) throws JSONException
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

	@Override
	public boolean equals(Object o)
	{
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		if (!super.equals(o)) return false;

		LongDataPoint that = (LongDataPoint) o;

		if (m_value != that.m_value) return false;

		return true;
	}

	@Override
	public int hashCode()
	{
		int result = super.hashCode();
		result = 31 * result + (int) (m_value ^ (m_value >>> 32));
		return result;
	}

	@Override
	public String toString()
	{
		return "LongDataPoint{" +
				"m_value=" + m_value +
				'}';
	}
}
