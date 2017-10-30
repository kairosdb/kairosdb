package org.kairosdb.datastore.cassandra.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Maps;
import com.google.inject.Inject;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class WriteBackRowKeyCache implements RowKeyCache {
    private final LoadingCache<ByteBuffer, Boolean> outerLayerCache;

    @Inject
    public WriteBackRowKeyCache(final RowKeyCacheConfiguration configuration, final GeneralByteBufferCacheStore rowKeyCacheStore) {
        this.outerLayerCache = Caffeine.newBuilder()
                .initialCapacity(configuration.getMaxSize()/3 + 1)
                .maximumSize(configuration.getMaxSize())
                .expireAfterWrite(configuration.getDefaultTtlInSeconds(), TimeUnit.HOURS)
                .writer(rowKeyCacheStore)
                .recordStats()
                .build(rowKeyCacheStore);
    }

    @Override
    public void put(@Nonnull final ByteBuffer rowKey) {
        this.outerLayerCache.put(rowKey, Boolean.TRUE);
    }

    @Override
    public void putAll(@Nonnull final Set<ByteBuffer> rowKeys) {
        this.outerLayerCache.putAll(Maps.asMap(rowKeys, input -> Boolean.TRUE));
    }

    @Override
    public boolean isKnown(@Nonnull final ByteBuffer rowKey) {
        return this.outerLayerCache.get(rowKey) != null;
    }
}
