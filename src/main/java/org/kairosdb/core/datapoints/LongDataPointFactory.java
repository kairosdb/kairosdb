package org.kairosdb.core.datapoints;

import org.kairosdb.core.DataPoint;

import java.nio.ByteBuffer;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 8/31/13
 Time: 7:25 AM
 To change this template use File | Settings | File Templates.
 */
public interface LongDataPointFactory extends DataPointFactory
{
	public DataPoint createDataPoint(long timestamp, long value);
}
