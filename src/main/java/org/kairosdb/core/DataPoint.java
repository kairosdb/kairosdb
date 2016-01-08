package org.kairosdb.core;

import org.json.JSONException;
import org.json.JSONWriter;
import org.kairosdb.core.datastore.DataPointGroup;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;



public interface DataPoint
{
	public static final String API_LONG = "long";
	public static final String API_DOUBLE = "double";

	public static final String GROUP_NUMBER = "number";

	public long getTimestamp();


	/**
	 Provides serialized form of data so it can be written to the data store
	 @return
	 */
	//public ByteBuffer toByteBuffer();

	public void writeValueToBuffer(DataOutput buffer) throws IOException;

	public void writeValueToJson(JSONWriter writer) throws JSONException;

	/**
		This is used to identify the data type on the wire in json format
	 */
	public String getApiDataType();

	/**
	 This is used to identify the data type in the data store.
	 The reason this is different from api data type is you may want to provide
	 a new implementation for storing long values.  So the api type may be 'long'
	 but the data store type may be 'long2'.  this way going forward new
	 incoming long values will be stored as 'long2' but you can still read both
	 'long' and 'long2' from the data store.
	 @return
	 */
	public String getDataStoreDataType();

	public boolean isLong();
	public long getLongValue();
	public boolean isDouble();
	public double getDoubleValue();
	public DataPointGroup getDataPointGroup();
	public void setDataPointGroup(DataPointGroup dataPointGroup);

}
