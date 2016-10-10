package org.kairosdb.core.datastore;

import org.kairosdb.core.DataPoint;

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
	public void addDataPoint(DataPoint datapoint) throws IOException;
	
	public void startDataPointSet(String dataType, Map<String, String> tags) throws IOException;
	public void endDataPoints() throws IOException;
}
