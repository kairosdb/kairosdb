package org.kairosdb.core.datapoints;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datapoints.FractionDataPoint;

import java.io.DataInput;
import java.io.IOException;
import static com.google.common.base.Preconditions.checkState;

public class FractionDataPointFactory implements DataPointFactory
{
	public static final String DST_FRACTION = "kairos_fraction";
	public static final String GROUP_TYPE = "fraction";

	@Override
	public String getDataStoreType()
	{
		return DST_FRACTION;
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
			long numerator = object.get("numerator").getAsLong();
			long denominator = object.get("denominator").getAsLong();
			checkState(denominator != 0);

			return new FractionDataPoint(timestamp, numerator, denominator);
		}
		else
			throw new IOException("JSON object is not a valid fraction data point");
	}

	@Override
	public DataPoint getDataPoint(long timestamp, DataInput buffer) throws IOException
	{
		long numerator = buffer.readLong();
		long denominator = buffer.readLong();

		return new FractionDataPoint(timestamp, numerator, denominator);
	}
}
