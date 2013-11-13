package org.kairosdb.core.datapoints;

import org.kairosdb.core.DataPoint;

import java.nio.ByteBuffer;

/**
 Implmementation must be thread safe.
 */
public interface DataPointFactory
{
	public String getDataStoreType();

	public DataPoint getDataPoint(long timestamp, String json);
	public DataPoint getDataPoint(long timestamp, ByteBuffer buffer);
}
