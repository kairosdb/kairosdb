package org.kairosdb.core.datastore;


import org.junit.Test;
import org.kairosdb.core.exception.DatastoreException;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

public class QueryLevelCachingSearchResultFactoryTest
{

    @Test
    public void testCreateSearchResult() throws DatastoreException
    {
        QueryLevelCachingSearchResultFactory factory = new QueryLevelCachingSearchResultFactory(new DummySearchResultFactory());
        QueryMetric queryMetricX = new QueryMetric(0, 0, "testMetricX");
        QueryMetric queryMetricY = new QueryMetric(0, 0, "testMetricY");
        DatastoreQueryContext contextA = DatastoreQueryContext.create("testA");
        SearchResult searchResultA = factory.createSearchResult(queryMetricX, contextA);

        DatastoreQueryContext contextB = DatastoreQueryContext.create("testB");
        SearchResult searchResultB = factory.createSearchResult(queryMetricX, contextB);

        // These two search results were created using different contexts, so they shouldn't be the same instance
        assertNotSame(searchResultA, searchResultB);

        // Retrieving the same QueryMetric using the same context should give the same (cached) instance
        assertSame(searchResultA, factory.createSearchResult(queryMetricX, contextA));
        assertSame(searchResultB, factory.createSearchResult(queryMetricX, contextB));

        // Retrieving a different QueryMetric in the same context should give a different result back
        assertNotSame(searchResultA, factory.createSearchResult(queryMetricY, contextA));
    }


    private static class DummySearchResultFactory implements SearchResultFactory {
        @Override
        public SearchResult createSearchResult(QueryMetric queryMetric, DatastoreQueryContext datastoreQueryContext) throws DatastoreException
        {
            return new MemorySearchResult(queryMetric.getName());
        }
    }
}