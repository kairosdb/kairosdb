package org.kairosdb.core.datapoints;

import org.kairosdb.core.DataPoint;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 8/31/13
 Time: 7:25 AM
 To change this template use File | Settings | File Templates.
 */
public interface DoubleDataPointFactory extends DataPointFactory
{
	public DataPoint createDataPoint(long timestamp, double value);
}
