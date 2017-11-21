package org.kairosdb.datastore.cassandra.cache;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.admin.CacheMetricsProvider;
import org.kairosdb.datastore.cassandra.cache.persistence.GeneralHashCacheStore;
import org.kairosdb.datastore.cassandra.cache.persistence.RedisWriteBackReadThroughCacheStore;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
    private CacheMetricsProvider cacheMetricsProvider;

    @Before
    public void setUp() throws Exception {
        configuration = mock(RowKeyCacheConfiguration.class);
        when(configuration.getMaxSize()).thenReturn(42);
        when(configuration.getTtlInSeconds()).thenReturn(42);
        cacheStore = mock(GeneralHashCacheStore.class);
        cacheMetricsProvider = mock(CacheMetricsProvider.class);
    }

    private DefaultRowKeyCache createDefaultCache() {
        return new DefaultRowKeyCache(cacheStore, cacheMetricsProvider, configuration, mock(Executor.class));
    }

    @Test
    public void testPut() {
        final DefaultRowKeyCache cache = createDefaultCache();
        final ByteBuffer given = ByteBuffer.wrap(new byte[]{42, 69});
        cache.put(given);
        verify(cacheStore).write(any(BigInteger.class), any());
    }

    @Test
    public void testGetHit() throws Exception {
        final DefaultRowKeyCache cache = createDefaultCache();
        final ByteBuffer given = ByteBuffer.wrap(new byte[]{42, 69});
        cache.put(given);
        assertTrue(cache.isKnown(given));
        verify(cacheStore, never()).load(any(BigInteger.class));
    }

    @Test
    public void testGetMissAsyncRefresh() throws Exception {
        final DefaultRowKeyCache cache = createDefaultCache();
        final ByteBuffer given = ByteBuffer.wrap(new byte[]{42, 69});
        when(cacheStore.asyncLoad(any(BigInteger.class), any())).thenReturn(mock(CompletableFuture.class));
        assertFalse(cache.isKnown(given));
        verify(cacheStore).asyncLoad(any(BigInteger.class), any());
    }
}