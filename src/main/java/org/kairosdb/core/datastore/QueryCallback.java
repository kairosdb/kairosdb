package org.kairosdb.core.datastore;

import java.io.IOException;
import java.util.Map;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 10/18/13
 Time: 1:02 PM
 To change this template use File | Settings | File Templates.
 */
public interface QueryCallback
{
	public void addDataPoint(long timestamp, long value) throws IOException;
	public void addDataPoint(long timestamp, double value) throws IOException;
	//public void addDataPoint(long timestamp, Object value);

	public void startDataPointSet(Map<String, String> tags) throws IOException;
	public void endDataPoints() throws IOException;
}
