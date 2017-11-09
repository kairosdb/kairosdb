package org.kairosdb.datastore.cassandra.cache;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.datastore.cassandra.cache.persistence.GeneralHashCacheStore;

import javax.annotation.Nonnull;
import java.math.BigInteger;
import java.nio.ByteBuffer;

public class DefaultRowKeyCache extends AbstractByteBufferCache implements RowKeyCache {
    public static final String ROW_KEY_CACHE = "rowKeyCache";
    private static final byte[] KEY_PREFIX = (ROW_KEY_CACHE + ":").getBytes();

    @Inject
    public DefaultRowKeyCache(@Named(ROW_KEY_CACHE) final GeneralHashCacheStore rowKeyCacheStore,
                              final RowKeyCacheConfiguration configuration) {
        super(rowKeyCacheStore, configuration.getMaxSize(), configuration.getTtlInSeconds());
    }

    @Override
    public void put(@Nonnull final ByteBuffer rowKey) {
        final ByteBuffer copy = rowKey.duplicate();
        final ByteBuffer prefixed = ByteBuffer.allocate(KEY_PREFIX.length + rowKey.limit())
                .put(KEY_PREFIX)
                .put(copy);
        final BigInteger hash = doubleHash(prefixed);
//        String metric = new String(copy.array());
//        metric = metric.substring(0, Math.min(32, metric.indexOf(0x0)));
        this.outerLayerCache.put(hash, rowKey);
    }

    @Override
    public boolean isKnown(@Nonnull final ByteBuffer rowKey) {
        final ByteBuffer copy = rowKey.duplicate();
        final ByteBuffer prefixed = ByteBuffer.allocate(KEY_PREFIX.length + rowKey.limit())
                .put(KEY_PREFIX)
                .put(copy);
        final BigInteger hash = doubleHash(prefixed);
        return this.outerLayerCache.get(hash) != null;
    }
}
