package org.kairosdb.core;

import java.nio.ByteBuffer;



public interface DataPoint extends Comparable<DataPoint>
{
	public static final String API_LONG = "long";
	public static final String API_DOUBLE = "double";

	public long getTimestamp();


	/**
	 Provides serialized form of data so it can be written to the data store
	 @return
	 */
	public ByteBuffer toByteBuffer();
	public String toString();

	/**
	 This is used when sorting data points within the tournament tree.
	 The sorting must be by timestamp.  Two objects *must not* be considered equal
	 or they will disappear in the storing process.
	 @param dp
	 @return Must return either 1 or -1, never 0 or data loss will occur.
	 */
	public int compareTo(DataPoint dp);

	/**
		This is used to identify the data type on the wire in json format
	 */
	public String getApiDataType();

	/**
	 This is used to identify the data type in the data store.
	 The reason this is different from api data type is you may want to provide
	 a new implementation for storing long values.  So the api type may be 'long'
	 but the data store type may be 'long2'.  this way going forward new
	 incomming long values will be stored as 'long2' but you can still read both
	 'long' and 'long2' from the data store.
	 @return
	 */
	public String getDataStoreDataType();

}
