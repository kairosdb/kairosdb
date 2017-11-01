package org.kairosdb.datastore.cassandra.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public abstract class AbstractByteBufferCache {
    protected final LoadingCache<ByteBuffer, Boolean> outerLayerCache;

    protected AbstractByteBufferCache(final GeneralByteBufferCacheStore cacheStore,
                                final int maxSize, final int ttlInSeconds) {
        this.outerLayerCache = Caffeine.newBuilder()
                .initialCapacity(maxSize / 3 + 1)
                .maximumSize(maxSize)
                .expireAfterWrite(ttlInSeconds, TimeUnit.HOURS)
                .writer(cacheStore)
                .recordStats()
                .build(cacheStore);
    }

}
