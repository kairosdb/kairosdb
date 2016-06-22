package org.kairosdb.datastore.cassandra;

import org.kairosdb.core.datastore.DatastoreMetricQuery;
import org.kairosdb.core.datastore.QueryPlugin;

import java.util.Collection;

/**
 Created by bhawkins on 11/23/14.
 */
public interface CassandraRowKeyPlugin extends QueryPlugin
{
	/**
	 Must return the row keys for a query grouped by time
	 @param query
	 @return
	 */
	Collection<DataPointsRowKey> getKeysForQueryIterator(DatastoreMetricQuery query);
}
