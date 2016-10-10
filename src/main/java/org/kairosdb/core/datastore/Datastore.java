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
	public void close() throws InterruptedException, DatastoreException;

	//public void putDataPoint(String metricName, ImmutableSortedMap<String, String> tags, DataPoint dataPoint, int ttl) throws DatastoreException;

	public Iterable<String> getMetricNames() throws DatastoreException;

	public Iterable<String> getTagNames() throws DatastoreException;

	public Iterable<String> getTagValues() throws DatastoreException;

	public void queryDatabase(DatastoreMetricQuery query, QueryCallback queryCallback) throws DatastoreException;

	public void deleteDataPoints(DatastoreMetricQuery deleteQuery) throws DatastoreException;

	public TagSet queryMetricTags(DatastoreMetricQuery query) throws DatastoreException;
}
