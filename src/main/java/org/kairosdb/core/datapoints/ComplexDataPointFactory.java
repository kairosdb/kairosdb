package org.kairosdb.core.datapoints;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.kairosdb.core.DataPoint;

import java.io.DataInput;
import java.io.IOException;

/**
 Used to show how to create a custom data type
 Created by bhawkins on 6/30/14.
 */
public class ComplexDataPointFactory implements DataPointFactory
{
	public static final String DST_COMPLEX = "kairos_complex";
	public static final String GROUP_TYPE = "complex";

	@Override
	public String getDataStoreType()
	{
		return DST_COMPLEX;
	}

	@Override
	public String getGroupType()
	{
		return GROUP_TYPE;
	}

	@Override
	public DataPoint getDataPoint(long timestamp, JsonElement json) throws IOException
	{
		if (json.isJsonObject())
		{
			JsonObject object = json.getAsJsonObject();
			double real = object.get("real").getAsDouble();
			double imaginary = object.get("imaginary").getAsDouble();

			return new ComplexDataPoint(timestamp, real, imaginary);
		}
		else
			throw new IOException("JSON object is not a valid complex data point");
	}

	@Override
	public DataPoint getDataPoint(long timestamp, DataInput buffer) throws IOException
	{
		double real = buffer.readDouble();
		double imaginary = buffer.readDouble();

		return new ComplexDataPoint(timestamp, real, imaginary);
	}
}
