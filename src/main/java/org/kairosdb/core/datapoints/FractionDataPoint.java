package org.kairosdb.core.datapoints;

import org.json.JSONException;
import org.json.JSONWriter;

import java.io.DataOutput;
import java.io.IOException;

public class FractionDataPoint extends DataPointHelper
{
	private static final String API_TYPE = "fraction";
	private long m_numerator;
	private long m_denominator;

	public FractionDataPoint(long timestamp, long numerator, long denominator)
	{
		super(timestamp);
		m_numerator = numerator;
		m_denominator = denominator;
	}

    public double getDoubleNumerator()
    {
	return (double)m_numerator;
    }
    public double getDoubleDenominator()
    {
	return (double)m_denominator;
    }
    
	@Override
	public void writeValueToBuffer(DataOutput buffer) throws IOException
	{
		buffer.writeLong(m_numerator);
		buffer.writeLong(m_denominator);
	}

	@Override
	public void writeValueToJson(JSONWriter writer) throws JSONException
	{
		writer.object();

		writer.key("numerator").value(m_numerator);
		writer.key("denominator").value(m_denominator);

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
		return FractionDataPointFactory.DST_FRACTION;
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
