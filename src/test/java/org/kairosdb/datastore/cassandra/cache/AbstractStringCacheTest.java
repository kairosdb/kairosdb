package org.kairosdb.datastore.cassandra.cache;

import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.admin.CacheMetricsProvider;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AbstractStringCacheTest {

    private RowKeyCacheConfiguration configuration;
    private CacheMetricsProvider cacheMetricsProvider;

    @Before
    public void setUp() throws Exception {
        configuration = mock(RowKeyCacheConfiguration.class);
        when(configuration.getMaxSize()).thenReturn(42);
        when(configuration.getTtlInSeconds()).thenReturn(42);
        cacheMetricsProvider = mock(CacheMetricsProvider.class);
    }

    @Test
    public void testGetHit() throws Exception {
        final AbstractStringCache cache = createDefaultCache();
        final String given = "fookey";
        cache.put(given);
        assertTrue(cache.isKnown(given));
    }
    
    @Test
    public void testGetMiss() throws Exception {
        final AbstractStringCache cache = createDefaultCache();
        final String given = "fookey";
        assertFalse(cache.isKnown(given));
    }


    private AbstractStringCache createDefaultCache() {
        return new AbstractStringCache(cacheMetricsProvider, configuration, "testCache") {
        };
    }

}
