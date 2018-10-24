package org.kairosdb.core.datastore;

import org.kairosdb.core.exception.DatastoreException;

/**
 * Factory for the creation of {@link SearchResult}s.
 */
public interface SearchResultFactory
{
	/**
	 * Create a named {@link SearchResult}
	 *
	 * @param queryMetric           the QueryMetric for which the SearchResult is to be created
	 * @param datastoreQueryContext shared context over multiple queries in a single operation
	 * @return the search result for the given query
	 */
	SearchResult createSearchResult(QueryMetric queryMetric, DatastoreQueryContext datastoreQueryContext) throws DatastoreException;

}
