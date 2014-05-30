package org.kairosdb.core.datapoints;

import com.google.gson.JsonElement;
import org.kairosdb.core.DataPoint;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.kairosdb.core.DataPoint.GROUP_NUMBER;

/**
 Created with Netbeans.
 User: lcoulet
 To change this template use File | Settings | File Templates.
 */
public class NullDataPointFactory implements DataPointFactory
{
	
	public static final String DATASTORE_TYPE = "null";
	public static final String API_TYPE = "null";

	

	public static void writeToByteBuffer(DataOutput buffer, NullDataPoint dataPoint) throws IOException
	{
		buffer.writeByte(0x0);
	}

	@Override
	public String getDataStoreType()
	{
		return DATASTORE_TYPE;
	}

	@Override
	public String getGroupType()
	{
		return GROUP_NUMBER;
	}

	@Override
	public DataPoint getDataPoint(long timestamp, JsonElement json)
	{
		return new NullDataPoint(timestamp);
	}

	@Override
	public DataPoint getDataPoint(long timestamp, DataInput buffer) throws IOException
	{
		
		return new NullDataPoint(timestamp);
	}
}
