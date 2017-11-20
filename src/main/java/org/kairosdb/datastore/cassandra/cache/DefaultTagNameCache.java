package org.kairosdb.datastore.cassandra.cache;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.admin.CacheMetricsProvider;
import org.kairosdb.datastore.cassandra.cache.persistence.GeneralHashCacheStore;

public class DefaultTagNameCache extends AbstractStringCache {
    public static final String TAG_NAME_CACHE = "tagNameCache";

    @Inject
    public DefaultTagNameCache(@Named(TAG_NAME_CACHE) final GeneralHashCacheStore cacheStore,
                               final CacheMetricsProvider cacheMetricsProvider,
                               final TagNameCacheConfiguration config) {
        super(cacheStore, cacheMetricsProvider, config, TAG_NAME_CACHE);
    }
}
