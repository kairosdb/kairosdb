//
// Datastore.java
//
// Copyright 2013, NextPage Inc. All rights reserved.
//

package org.kairosdb.core.datastore;

import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.exception.DatastoreException;

import java.util.List;

public interface Datastore
{
	public void close() throws InterruptedException, DatastoreException;

	public void putDataPoints(DataPointSet dps) throws DatastoreException;

	public Iterable<String> getMetricNames() throws DatastoreException;

	public Iterable<String> getTagNames() throws DatastoreException;

	public Iterable<String> getTagValues() throws DatastoreException;

	public List<DataPointRow> queryDatabase(DatastoreMetricQuery query, CachedSearchResult cachedSearchResult) throws DatastoreException;

	public void deleteDataPoints(DatastoreMetricQuery deleteQuery, CachedSearchResult cachedSearchResult) throws DatastoreException;

	TagSet queryMetricTags(DatastoreMetricQuery query) throws DatastoreException;
}
