package org.kairosdb.core.datastore;

import org.kairosdb.core.DataPoint;

import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 10/18/13
 Time: 1:02 PM
 To change this template use File | Settings | File Templates.
 */
public interface QueryCallback
{
	DataPointWriter startDataPointSet(String dataType, SortedMap<String, String> tags) throws IOException;

	interface DataPointWriter extends AutoCloseable
	{
		void addDataPoint(DataPoint datapoint) throws IOException;
		void close() throws IOException;
	}
}
