package org.kairosdb.datastore.cassandra.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import org.kairosdb.core.admin.CacheMetricsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static com.google.common.hash.Hashing.murmur3_128;
import static net.openhft.hashing.LongHashFunction.xx;

public abstract class AbstractByteBufferCache {
    private final Logger LOG = LoggerFactory.getLogger(AbstractByteBufferCache.class);

    private static final int MURMUR_SEED = 0xDEADBEEF;
    private static final int XX_SEED = 0xCAFEBABE;

    @VisibleForTesting
    final Cache<BigInteger, Object> internalCache;

    @VisibleForTesting
    AbstractByteBufferCache(final CacheMetricsProvider cacheMetricsProvider,
                                final int maxSize, final int ttlInSeconds, final String cacheId) {
        this.internalCache = Caffeine.newBuilder()
                .initialCapacity(maxSize / 3 + 1)
                .maximumSize(maxSize)
                .expireAfterWrite(ttlInSeconds, TimeUnit.SECONDS)
                .recordStats()
                .build();
        cacheMetricsProvider.registerCache(cacheId, this.internalCache);
    }

    @VisibleForTesting
    BigInteger doubleHash(final ByteBuffer payload) {
        final long murmurHash = murmur3_128(MURMUR_SEED).hashBytes(payload.array()).asLong();
        final long xxHash = xx(XX_SEED).hashBytes(payload);
        final ByteBuffer doubleHash = ByteBuffer.allocate((Long.SIZE * 2) / 8)
                .putLong(murmurHash)
                .putLong(xxHash);
        doubleHash.flip();
        return new BigInteger(doubleHash.array());
    }

    public void put(@Nonnull final ByteBuffer key) {
        final BigInteger hash = doubleHash(key);
        this.internalCache.put(hash, key);
    }

    public boolean isKnown(final ByteBuffer key) {
        final BigInteger hash = doubleHash(key);
        try {
            return this.internalCache.getIfPresent(hash) != null;
        } catch (Exception e) {
            LOG.error("failed to read cached entry with key {} ({})", key, hash);
        }
        return false;

    }
}
