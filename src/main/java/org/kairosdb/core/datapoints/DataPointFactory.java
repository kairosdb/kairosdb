package org.kairosdb.core.datapoints;

import com.google.gson.JsonElement;
import org.kairosdb.core.DataPoint;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 Implmementation must be thread safe.
 */
public interface DataPointFactory
{
	/**
	 This returns the type string that represents the data as it is packed in
	 binary form.  The string returned from this call will be stored with the
	 data and used to locate the appropriate factory to marshal the data when
	 retrieving data from the datastore.
	 @return
	 */
	public String getDataStoreType();

	/**
	 This really is for aggregation purposes.  We know if an aggregator can handle
	 this type by checking the group type against the aggregator by calling
	 Aggregator.canAggregate().

	 As of this writing there are two group types used inside Kairos 'number' and
	 'text'.  This is free formed and you can make up your own.
	 @return
	 */
	public String getGroupType();

	/**
	 This returns the data type that is in the json serialized form of the datapoints
	 returned by this factory.  The value returned by this method does not determine
	 if this factory is used to deserialize json, that id determined by registering
	 the factory in the kairosdb.properteis file.

	 Example return values would be 'long', 'double' or 'string'
	 @return
	 */
	//public String getAPIType();

	public DataPoint getDataPoint(long timestamp, JsonElement json) throws IOException;
	public DataPoint getDataPoint(long timestamp, DataInput buffer) throws IOException;
}
