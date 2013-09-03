package org.kairosdb.core.datapoints;

import org.kairosdb.core.DataPoint;

import java.nio.ByteBuffer;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 9/2/13
 Time: 5:03 AM
 To change this template use File | Settings | File Templates.
 */
public interface DataPointFactory
{
	public String getDataStoreType();

	public DataPoint getDataPoint(long timestamp, String json);
	public DataPoint getDataPoint(long timestamp, ByteBuffer buffer);
}
