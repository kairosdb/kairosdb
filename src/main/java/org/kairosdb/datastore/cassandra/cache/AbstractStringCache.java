package org.kairosdb.datastore.cassandra.cache;

import com.google.common.annotations.VisibleForTesting;
import org.kairosdb.core.admin.CacheMetricsProvider;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public abstract class AbstractStringCache extends AbstractByteBufferCache implements StringKeyCache {
    private static Charset UTF8 = Charset.forName("UTF-8");

    @VisibleForTesting
    AbstractStringCache(final CacheMetricsProvider cacheMetricsProvider,
                                  final CacheConfiguration config, final String cacheId) {
        super(cacheMetricsProvider, config.getMaxSize(), config.getTtlInSeconds(), cacheId);
    }

    @Override
    public void put(@Nonnull String key) {
        final ByteBuffer metric = ByteBuffer.wrap(key.getBytes(UTF8));
        super.put(metric);
    }

    @Override
    public boolean isKnown(@Nonnull String key) {
        final ByteBuffer metric = ByteBuffer.wrap(key.getBytes(UTF8));
        return super.isKnown(metric);
    }
}
