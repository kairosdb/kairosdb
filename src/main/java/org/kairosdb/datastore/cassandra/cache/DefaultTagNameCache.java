package org.kairosdb.datastore.cassandra.cache;

import com.google.inject.Inject;
import org.kairosdb.core.admin.CacheMetricsProvider;

public class DefaultTagNameCache extends AbstractStringCache {
    public static final String TAG_NAME_CACHE = "tagNameCache";

    @Inject
    public DefaultTagNameCache(final CacheMetricsProvider cacheMetricsProvider,
                               final TagNameCacheConfiguration config) {
        super(cacheMetricsProvider, config, TAG_NAME_CACHE);
    }
}
