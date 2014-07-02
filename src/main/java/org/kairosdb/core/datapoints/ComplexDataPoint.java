package org.kairosdb.core.datapoints;

import org.json.JSONException;
import org.json.JSONWriter;

import java.io.DataOutput;
import java.io.IOException;

/**
 Used to show how to create a custom data type
 Created by bhawkins on 6/27/14.
 */
public class ComplexDataPoint extends DataPointHelper
{
	private static final String API_TYPE = "complex";
	private double m_real;
	private double m_imaginary;

	public ComplexDataPoint(long timestamp, double real, double imaginary)
	{
		super(timestamp);
		m_real = real;
		m_imaginary = imaginary;
	}

	@Override
	public void writeValueToBuffer(DataOutput buffer) throws IOException
	{
		buffer.writeDouble(m_real);
		buffer.writeDouble(m_imaginary);
	}

	@Override
	public void writeValueToJson(JSONWriter writer) throws JSONException
	{
		writer.object();

		writer.key("real").value(m_real);
		writer.key("imaginary").value(m_imaginary);

		writer.endObject();
	}

	@Override
	public String getApiDataType()
	{
		return API_TYPE;
	}

	@Override
	public String getDataStoreDataType()
	{
		return ComplexDataPointFactory.DST_COMPLEX;
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
}
