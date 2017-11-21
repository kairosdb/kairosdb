package org.kairosdb.datastore.cassandra.cache;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.admin.CacheMetricsProvider;
import org.kairosdb.datastore.cassandra.cache.persistence.GeneralHashCacheStore;

import java.util.concurrent.Executor;

import static org.kairosdb.datastore.cassandra.cache.AsyncCacheExecutor.CACHE_EXECUTOR;

public class DefaultTagValueCache extends AbstractStringCache {
    public static final String TAG_VALUE_CACHE = "tagValueCache";

    @Inject
    public DefaultTagValueCache(@Named(TAG_VALUE_CACHE) final GeneralHashCacheStore cacheStore,
                                final CacheMetricsProvider cacheMetricsProvider,
                                final TagValueCacheConfiguration config,
                                @Named(CACHE_EXECUTOR) final Executor executor) {
        super(cacheStore, cacheMetricsProvider, config, TAG_VALUE_CACHE, executor);
    }
}
