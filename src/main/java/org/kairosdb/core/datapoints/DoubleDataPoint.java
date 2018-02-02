package org.kairosdb.core.datapoints;


import org.json.JSONException;
import org.json.JSONWriter;

import java.io.DataOutput;
import java.io.IOException;

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

	/*@Override
	public ByteBuffer toByteBuffer()
	{
		return DoubleDataPointFactoryImpl.writeToByteBuffer(this);
	}*/

	@Override
	public void writeValueToBuffer(DataOutput buffer) throws IOException
	{
		DoubleDataPointFactoryImpl.writeToByteBuffer(buffer, this);
	}

	@Override
	public void writeValueToJson(JSONWriter writer) throws JSONException
	{
		//m_value will not equal itself if it is Double.NaN.  Weird I know but that is how it is.
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

	@Override
	public boolean equals(Object o)
	{
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		if (!super.equals(o)) return false;

		DoubleDataPoint that = (DoubleDataPoint) o;

		if (Double.compare(that.m_value, m_value) != 0) return false;

		return true;
	}

	@Override
	public int hashCode()
	{
		int result = super.hashCode();
		long temp = Double.doubleToLongBits(m_value);
		result = 31 * result + (int) (temp ^ (temp >>> 32));
		return result;
	}
}
