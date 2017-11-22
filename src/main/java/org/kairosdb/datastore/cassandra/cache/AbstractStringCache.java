package org.kairosdb.datastore.cassandra.cache;

import org.kairosdb.core.admin.CacheMetricsProvider;
import org.kairosdb.datastore.cassandra.cache.persistence.GeneralHashCacheStore;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.concurrent.Executor;

import static org.slf4j.LoggerFactory.getLogger;

public abstract class AbstractStringCache extends AbstractByteBufferCache implements StringKeyCache {
    private static final Logger LOG = getLogger(AbstractStringCache.class);
    private static Charset UTF8 = Charset.forName("UTF-8");

    protected AbstractStringCache(final GeneralHashCacheStore cacheStore, final CacheMetricsProvider cacheMetricsProvider,
                                  final CacheConfiguration config, final String cacheId,
                                  final Executor executor) {
        super(cacheStore, cacheMetricsProvider, config.getMaxSize(), config.getTtlInSeconds(), cacheId, executor);
    }

    @Override
    public void put(@Nonnull String key) {
        final ByteBuffer metric = ByteBuffer.wrap(key.getBytes(UTF8));
        final BigInteger hash = doubleHash(metric);
        try {
            this.outerLayerCache.put(hash, DUMMY_PAYLOAD);
        } catch (Exception e) {
            LOG.error("failed to write cached entry with key {} ({}): {}", key, hash, e.getMessage());
        }
    }

    @Override
    public boolean isKnown(@Nonnull String key) {
        final ByteBuffer metric = ByteBuffer.wrap(key.getBytes(UTF8));
        return super.isKnown(metric);
    }
}
