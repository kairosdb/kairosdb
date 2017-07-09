package org.kairosdb.datastore.cassandra;

import org.kairosdb.core.DataPoint;

import java.io.IOException;

/**
 Created by bhawkins on 12/12/16.
 */
public interface BatchClient
{
	void addRowKey(String metricName, DataPointsRowKey rowKey, int rowKeyTtl);

	void addMetricName(String metricName);

	void addTagName(String tagName);

	void addTagValue(String value);

	void addDataPoint(DataPointsRowKey rowKey, int columnTime, DataPoint dataPoint, int ttl) throws IOException;

	void submitBatch();
}
