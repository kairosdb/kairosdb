package org.kairosdb.core;

import org.json.JSONException;
import org.json.JSONWriter;
import org.kairosdb.core.datastore.DataPointGroup;

import java.io.DataOutput;
import java.io.IOException;



public interface DataPoint
{
	String API_LONG = "long";
	String API_DOUBLE = "double";

	String GROUP_NUMBER = "number";

	long getTimestamp();

	void setTimestamp(long timestamp);

	void writeValueToBuffer(DataOutput buffer) throws IOException;

	void writeValueToJson(JSONWriter writer) throws JSONException;

	/**
		This is used to identify the data type on the wire in json format
	 @return api data type used in json
	 */
	String getApiDataType();

	/**
	 This is used to identify the data type in the data store.
	 The reason this is different from api data type is you may want to provide
	 a new implementation for storing long values.  So the api type may be 'long'
	 but the data store type may be 'long2'.  this way going forward new
	 incoming long values will be stored as 'long2' but you can still read both
	 'long' and 'long2' from the data store.
	 @return data store type
	 */
	String getDataStoreDataType();

	boolean isLong();
	long getLongValue();
	boolean isDouble();
	double getDoubleValue();

	DataPointGroup getDataPointGroup();

	void setDataPointGroup(DataPointGroup dataPointGroup);

}
