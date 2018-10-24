package org.kairosdb.core.datastore;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.exception.DatastoreException;

import java.util.List;
import java.util.Objects;

/**
 * Wrapper around a {@link SearchResultFactory} claims created {@link SearchResult}s in the
 * {@link DatastoreQueryContext} so that the same results can be used within a single operation.
 * <p>
 * This improves both performance (so that a given underlying datastore query is only performed once for a given
 * composite operation) as well as improving correctness (as the results from a single composite query contain the same
 * view of underlying data).
 */
public class QueryLevelCachingSearchResultFactory implements SearchResultFactory
{

    private final SearchResultFactory delegate;

    @Inject
    public QueryLevelCachingSearchResultFactory(@Named("BaseImplementation") SearchResultFactory delegate)
    {
        this.delegate = delegate;
    }

    @Override
    public SearchResult createSearchResult(QueryMetric queryMetric, DatastoreQueryContext datastoreQueryContext) throws DatastoreException
    {
        QueryMetricKey queryMetricKey = new QueryMetricKey(queryMetric.getCacheString());
        WrappedSearchResult wrappedSearchResult = (WrappedSearchResult) datastoreQueryContext.getData(queryMetricKey);
        if (wrappedSearchResult != null) {
            return wrappedSearchResult.getSearchResult();
        } else {
            SearchResult searchResult = delegate.createSearchResult(queryMetric, datastoreQueryContext);
            datastoreQueryContext.setData(
                    queryMetricKey,
                    new WrappedSearchResult(searchResult));
            return searchResult;
        }
    }

    private static class QueryMetricKey
    {
        private final String queryMetricIdentifier;

        public QueryMetricKey(String queryMetricIdentifier)
        {
            this.queryMetricIdentifier = queryMetricIdentifier;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            QueryMetricKey that = (QueryMetricKey) o;
            return Objects.equals(queryMetricIdentifier, that.queryMetricIdentifier);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(queryMetricIdentifier);
        }
    }

    private static class WrappedSearchResult implements AutoCloseable
    {
        private final SearchResult searchResult;
        private final List<DataPointRow> dataPointRows;

        public WrappedSearchResult(SearchResult searchResult)
        {
            this.searchResult = searchResult;
            this.dataPointRows = searchResult.getRows();
        }

        public SearchResult getSearchResult()
        {
            return searchResult;
        }

        @Override
        public void close()
        {
            for (DataPointRow dataPointRow : this.dataPointRows) {
                dataPointRow.close();
            }
        }
    }
}
