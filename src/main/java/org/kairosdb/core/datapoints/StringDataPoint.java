package org.kairosdb.core.datapoints;

import org.json.JSONException;
import org.json.JSONWriter;

import java.io.DataOutput;
import java.io.IOException;

/**
 Created by bhawkins on 12/14/13.
 */
public class StringDataPoint extends DataPointHelper
{
	public static final String API_TYPE = "string";

	private final String m_value;

	public StringDataPoint(long timestamp, String value)
	{
		super(timestamp);
		m_value = value;
	}

	@Override
	public void writeValueToBuffer(DataOutput buffer) throws IOException
	{
		buffer.writeUTF(m_value);
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
}
