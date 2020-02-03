package org.kairosdb.core.datapoints;

import org.json.JSONException;
import org.json.JSONWriter;
import org.xerial.snappy.Snappy;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 Created by bhawkins on 09/15/2018
 */
public class SnappyStringDataPoint extends DataPointHelper
{
	public static final String API_TYPE = "string";

	private final String m_value;

	public SnappyStringDataPoint(long timestamp, String value)
	{
		super(timestamp);
		m_value = value;
	}

	@Override
	public void writeValueToBuffer(DataOutput buffer) throws IOException
	{
		byte[] compressedBytes = Snappy.compress(m_value);
		buffer.writeShort(compressedBytes.length);
		buffer.write(compressedBytes);
	}

	@Override
	public void writeValueToJson(JSONWriter writer) throws JSONException
	{
		writer.value(m_value);
	}

	@Override
	public String getApiDataType()
	{
		return API_TYPE;
	}

	@Override
	public String getDataStoreDataType()
	{
		return StringDataPointFactory.DST_STRING;
	}

	@Override
	public boolean isLong()
	{
		return false;
	}

	@Override
	public long getLongValue()
	{
		return 0;
	}

	@Override
	public boolean isDouble()
	{
		return false;
	}

	@Override
	public double getDoubleValue()
	{
		return 0;
	}

	public String getValue()
	{
		return (m_value);
	}

	@Override
	public boolean equals(Object o)
	{
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		if (!super.equals(o)) return false;
		SnappyStringDataPoint that = (SnappyStringDataPoint) o;
		return Objects.equals(m_value, that.m_value);
	}

	@Override
	public int hashCode()
	{
		return Objects.hash(super.hashCode(), m_value);
	}
}
