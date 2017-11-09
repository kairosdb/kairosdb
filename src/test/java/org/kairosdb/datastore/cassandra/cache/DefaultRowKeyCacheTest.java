package org.kairosdb.datastore.cassandra.cache;

import org.junit.Before;
import org.junit.Test;
import org.kairosdb.datastore.cassandra.cache.persistence.GeneralHashCacheStore;

import java.math.BigInteger;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultRowKeyCacheTest {

    private RowKeyCacheConfiguration configuration;
    private GeneralHashCacheStore cacheStore;

    @Before
    public void setUp() throws Exception {
        configuration = mock(RowKeyCacheConfiguration.class);
        when(configuration.getMaxSize()).thenReturn(42);
        when(configuration.getTtlInSeconds()).thenReturn(42);
        cacheStore = mock(GeneralHashCacheStore.class);
    }

    @Test
    public void testPut() {
        final DefaultRowKeyCache cache = new DefaultRowKeyCache(cacheStore, configuration);
        final ByteBuffer given = ByteBuffer.wrap(new byte[]{42, 69});
        cache.put(given);
        verify(cacheStore).write(any(BigInteger.class), any());
    }

    @Test
    public void testGetHit() throws Exception {
        final DefaultRowKeyCache cache = new DefaultRowKeyCache(cacheStore, configuration);
        final ByteBuffer given = ByteBuffer.wrap(new byte[]{42, 69});
        cache.put(given);
        assertTrue(cache.isKnown(given));
        verify(cacheStore, never()).load(any(BigInteger.class));
    }

    @Test
    public void testGetHitReadThrough() throws Exception {
        when(cacheStore.load(any(BigInteger.class))).thenReturn(Boolean.TRUE);
        final DefaultRowKeyCache cache = new DefaultRowKeyCache(cacheStore, configuration);
        final ByteBuffer given = ByteBuffer.wrap(new byte[]{42, 69});
        assertTrue(cache.isKnown(given));
        verify(cacheStore).load(any(BigInteger.class));
    }

    @Test
    public void testGetMiss() throws Exception {
        final DefaultRowKeyCache cache = new DefaultRowKeyCache(cacheStore, configuration);
        final ByteBuffer given = ByteBuffer.wrap(new byte[]{42, 69});
        assertFalse(cache.isKnown(given));
        verify(cacheStore).load(any(BigInteger.class));
    }
}