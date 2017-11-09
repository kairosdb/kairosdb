package org.kairosdb.datastore.cassandra.cache;

import com.google.inject.Inject;
import org.kairosdb.datastore.cassandra.cache.persistence.GeneralHashCacheStore;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import static org.slf4j.LoggerFactory.getLogger;

public abstract class AbstractStringCache extends AbstractByteBufferCache implements StringKeyCache {
    private static final Logger LOG = getLogger(AbstractStringCache.class);
    private static Charset UTF8 = Charset.forName("UTF-8");

    @Inject
    protected AbstractStringCache(final GeneralHashCacheStore cacheStore, final CacheConfiguration config) {
        super(cacheStore, config.getMaxSize(), config.getTtlInSeconds());
    }

    @Override
    public void put(@Nonnull String key) {
        final ByteBuffer metric = ByteBuffer.wrap(key.getBytes(UTF8));
        final BigInteger hash = doubleHash(metric);
        try {
            this.outerLayerCache.put(hash, key);
        } catch (Exception e) {
            LOG.error("failed to write cached entry with key {} ({})", key, hash);
        }
    }

    @Override
    public boolean isKnown(@Nonnull String key) {
        final ByteBuffer metric = ByteBuffer.wrap(key.getBytes(UTF8));
        final BigInteger hash = doubleHash(metric);
        try {
            return this.outerLayerCache.get(hash) != null;
        } catch (Exception e) {
            LOG.error("failed to read cached entry with key {} ({})", key, hash);
        }
        return false;
    }
}
