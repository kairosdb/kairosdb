package org.kairosdb.datastore.cassandra.cache;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.admin.CacheMetricsProvider;
import org.kairosdb.datastore.cassandra.cache.persistence.GeneralHashCacheStore;

import javax.annotation.Nonnull;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.concurrent.Executor;

import static org.kairosdb.datastore.cassandra.cache.AsyncCacheExecutor.CACHE_EXECUTOR;

public class DefaultRowKeyCache extends AbstractByteBufferCache implements RowKeyCache {
    public static final String ROW_KEY_CACHE = "rowKeyCache";
    private static final byte[] KEY_PREFIX = (ROW_KEY_CACHE + ":").getBytes();

    @Inject
    public DefaultRowKeyCache(@Named(ROW_KEY_CACHE) final GeneralHashCacheStore rowKeyCacheStore,
                              final CacheMetricsProvider cacheMetricsProvider,
                              final RowKeyCacheConfiguration configuration,
                              @Named(CACHE_EXECUTOR) final Executor executor) {
        super(rowKeyCacheStore, cacheMetricsProvider, configuration.getMaxSize(), configuration.getTtlInSeconds(),
                ROW_KEY_CACHE, executor);
    }

    @Override
    public void put(@Nonnull final ByteBuffer rowKey) {
        final ByteBuffer prefixed = prefixedBufferCopy(rowKey);
        final BigInteger hash = doubleHash(prefixed);
        this.outerLayerCache.put(hash, DUMMY_PAYLOAD);
    }

    @Override
    public boolean isKnown(@Nonnull final ByteBuffer rowKey) {
        final ByteBuffer prefixed = prefixedBufferCopy(rowKey);
        final BigInteger hash = doubleHash(prefixed);
        return this.outerLayerCache.get(hash) != null;
    }

    private ByteBuffer prefixedBufferCopy(final @Nonnull ByteBuffer rowKey) {
        final ByteBuffer copy = rowKey.duplicate();
        return ByteBuffer.allocate(KEY_PREFIX.length + rowKey.limit())
                .put(KEY_PREFIX)
                .put(copy);
    }
}
