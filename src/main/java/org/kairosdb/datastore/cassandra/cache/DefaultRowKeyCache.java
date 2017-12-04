package org.kairosdb.datastore.cassandra.cache;

import com.google.inject.Inject;
import org.kairosdb.core.admin.CacheMetricsProvider;

public class DefaultRowKeyCache extends AbstractByteBufferCache implements RowKeyCache {
    public static final String ROW_KEY_CACHE = "rowKeyCache";

    @Inject
    public DefaultRowKeyCache(final CacheMetricsProvider cacheMetricsProvider,
                              final RowKeyCacheConfiguration configuration) {
        super(cacheMetricsProvider, configuration.getMaxSize(), configuration.getTtlInSeconds(), ROW_KEY_CACHE);
    }
}
