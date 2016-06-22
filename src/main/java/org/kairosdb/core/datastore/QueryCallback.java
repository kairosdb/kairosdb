package org.kairosdb.core.datastore;

import java.io.IOException;
import java.util.Map;
import org.kairosdb.core.DataPoint;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 10/18/13
 Time: 1:02 PM
 To change this template use File | Settings | File Templates.
 */
public interface QueryCallback
{
	void addDataPoint(DataPoint datapoint) throws IOException;
	void startDataPointSet(String dataType, Map<String, String> tags) throws IOException;
	void endDataPoints() throws IOException;
}
