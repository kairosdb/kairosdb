//
// Datastore.java
//
// Copyright 2013, NextPage Inc. All rights reserved.
//

package org.kairosdb.core.datastore;

import org.kairosdb.core.exception.DatastoreException;

public interface Datastore
{
	void close() throws InterruptedException, DatastoreException;

	//public void putDataPoint(String metricName, ImmutableSortedMap<String, String> tags, DataPoint dataPoint, int ttl) throws DatastoreException;

	Iterable<String> getMetricNames() throws DatastoreException;

	Iterable<String> getTagNames() throws DatastoreException;

	Iterable<String> getTagValues() throws DatastoreException;

	void queryDatabase(DatastoreMetricQuery query, QueryCallback queryCallback) throws DatastoreException;

	void deleteDataPoints(DatastoreMetricQuery deleteQuery) throws DatastoreException;

	TagSet queryMetricTags(DatastoreMetricQuery query) throws DatastoreException;

	void setValue(String service, String serviceKey, String key, String value) throws DatastoreException;

	String getValue(String service, String serviceKey, String key) throws DatastoreException;

	Iterable<String> listServiceKeys(String service) throws DatastoreException;

	Iterable<String> listKeys(String service, String serviceKey) throws DatastoreException;

	Iterable<String> listKeys(String service, String serviceKey, String keyStartsWith) throws DatastoreException;

    void deleteKey(String service, String serviceKey, String key) throws DatastoreException;
}
