package org.kairosdb.datastore.cassandra.cache;

import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.admin.CacheMetricsProvider;
import org.kairosdb.datastore.cassandra.cache.persistence.GeneralHashCacheStore;

import java.math.BigInteger;
import java.util.concurrent.Executor;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class AbstractStringCacheTest {

    private RowKeyCacheConfiguration configuration;
    private GeneralHashCacheStore cacheStore;
    private CacheMetricsProvider cacheMetricsProvider;

    @Before
    public void setUp() throws Exception {
        configuration = mock(RowKeyCacheConfiguration.class);
        when(configuration.getMaxSize()).thenReturn(42);
        when(configuration.getTtlInSeconds()).thenReturn(42);
        cacheStore = mock(GeneralHashCacheStore.class);
        cacheMetricsProvider = mock(CacheMetricsProvider.class);
    }

    @Test
    public void testPut() {
        final AbstractStringCache cache = createDefaultCache();
        final String given = "fookey";
        cache.put(given);
        verify(cacheStore).write(any(BigInteger.class), any());
    }

    @Test
    public void testGetHit() throws Exception {
        final AbstractStringCache cache = createDefaultCache();
        final String given = "fookey";
        cache.put(given);
        assertTrue(cache.isKnown(given));
        verify(cacheStore, never()).load(any(BigInteger.class));
    }
    
    @Test
    public void testGetMiss() throws Exception {
        final AbstractStringCache cache = createDefaultCache();
        final String given = "fookey";
        assertFalse(cache.isKnown(given));
        verify(cacheStore).load(any(BigInteger.class));
    }


    private AbstractStringCache createDefaultCache() {
        return new AbstractStringCache(cacheStore, cacheMetricsProvider, configuration, "testCache", mock(Executor.class)) {
        };
    }

}
