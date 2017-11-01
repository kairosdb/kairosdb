package org.kairosdb.datastore.cassandra.cache;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;

public class DefaultRowKeyCache extends AbstractByteBufferCache implements RowKeyCache {
    public static final String ROW_KEY_CACHE = "rowKeyCache";

    @Inject
    public DefaultRowKeyCache(@Named(ROW_KEY_CACHE) final GeneralByteBufferCacheStore rowKeyCacheStore,
                              final RowKeyCacheConfiguration configuration) {
        super(rowKeyCacheStore, configuration.getMaxSize(), configuration.getTtlInSeconds());
    }

    @Override
    public void put(@Nonnull final ByteBuffer rowKey) {
        this.outerLayerCache.put(rowKey, Boolean.TRUE);
    }

    @Override
    public boolean isKnown(@Nonnull final ByteBuffer rowKey) {
        return this.outerLayerCache.get(rowKey) != null;
    }
}
