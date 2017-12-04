package org.kairosdb.datastore.cassandra.cache;

import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.admin.CacheMetricsProvider;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultRowKeyCacheTest {

    private RowKeyCacheConfiguration configuration;
    private CacheMetricsProvider cacheMetricsProvider;

    @Before
    public void setUp() throws Exception {
        configuration = mock(RowKeyCacheConfiguration.class);
        when(configuration.getMaxSize()).thenReturn(42);
        when(configuration.getTtlInSeconds()).thenReturn(42);
        cacheMetricsProvider = mock(CacheMetricsProvider.class);
    }

    private DefaultRowKeyCache createDefaultCache() {
        return new DefaultRowKeyCache(cacheMetricsProvider, configuration);
    }

    @Test
    public void testGetHit() throws Exception {
        final DefaultRowKeyCache cache = createDefaultCache();
        final ByteBuffer given = ByteBuffer.wrap(new byte[]{42, 69});
        cache.put(given);
        assertTrue(cache.isKnown(given));
    }

    @Test
    public void testGetMiss() throws Exception {
        final DefaultRowKeyCache cache = createDefaultCache();
        final ByteBuffer given = ByteBuffer.wrap(new byte[]{42, 69});
        assertFalse(cache.isKnown(given));
    }
}