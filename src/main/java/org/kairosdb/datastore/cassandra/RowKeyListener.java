package org.kairosdb.datastore.cassandra;

/**
 Created by bhawkins on 11/7/14.
 */
public interface RowKeyListener
{
	void addRowKey(String metricName, DataPointsRowKey rowKey, int rowKeyTtl);
}
