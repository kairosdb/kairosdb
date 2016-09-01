package org.kairosdb.core.datapoints;

import com.google.gson.JsonElement;
import org.kairosdb.core.DataPoint;

import java.io.DataInput;
import java.io.IOException;

/**
 Created by bhawkins on 12/14/13.
 */
public class StringDataPointFactory implements DataPointFactory
{
	public static final String DST_STRING = "kairos_string";
	public static final String GROUP_TYPE = "text";

	@Override
	public String getDataStoreType()
	{
		return DST_STRING;
	}

	@Override
	public String getGroupType()
	{
		return GROUP_TYPE;
	}

	@Override
	public DataPoint getDataPoint(long timestamp, JsonElement json) throws IOException
	{
		StringDataPoint ret = new StringDataPoint(timestamp, json.getAsString());
		return ret;
	}

	@Override
	public DataPoint getDataPoint(long timestamp, DataInput buffer) throws IOException
	{
		StringDataPoint ret = new StringDataPoint(timestamp, buffer.readUTF());
		return ret;
	}

	public DataPoint createDataPoint(long timestamp, String value)
	{
		StringDataPoint ret = new StringDataPoint(timestamp, value);
		return ret;
	}
}
