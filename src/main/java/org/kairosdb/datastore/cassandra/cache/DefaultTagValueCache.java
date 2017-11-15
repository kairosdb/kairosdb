package org.kairosdb.datastore.cassandra.cache;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.admin.CacheMetricsProvider;
import org.kairosdb.datastore.cassandra.cache.persistence.GeneralHashCacheStore;

public class DefaultTagValueCache extends AbstractStringCache {
    public static final String TAG_VALUE_CACHE = "tagValueCache";

    @Inject
    public DefaultTagValueCache(@Named(TAG_VALUE_CACHE) final GeneralHashCacheStore cacheStore,
                                final CacheMetricsProvider cacheMetricsProvider,
                                final TagValueCacheConfiguration config) {
        super(cacheStore, cacheMetricsProvider, config, TAG_VALUE_CACHE);
    }
}
