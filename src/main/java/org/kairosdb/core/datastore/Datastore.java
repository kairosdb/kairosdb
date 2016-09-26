//
// Datastore.java
//
// Copyright 2013, NextPage Inc. All rights reserved.
//

package org.kairosdb.core.datastore;

import com.google.common.collect.ImmutableSortedMap;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.exception.DatastoreException;

public interface Datastore
{
	void close() throws InterruptedException, DatastoreException;

	void putDataPoint(String metricName, ImmutableSortedMap<String, String> tags, DataPoint dataPoint, int ttl) throws DatastoreException;

	Iterable<String> getMetricNames() throws DatastoreException;

	Iterable<String> getTagNames() throws DatastoreException;

	Iterable<String> getTagValues() throws DatastoreException;

	void queryDatabase(DatastoreMetricQuery query, QueryCallback queryCallback) throws DatastoreException;

	void deleteDataPoints(DatastoreMetricQuery deleteQuery) throws DatastoreException;

	TagSet queryMetricTags(DatastoreMetricQuery query) throws DatastoreException;
}
