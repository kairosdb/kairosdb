package org.kairosdb.datastore.cassandra.cache;

import com.google.inject.Inject;
import org.kairosdb.core.admin.CacheMetricsProvider;

public class DefaultTagValueCache extends AbstractStringCache {
    public static final String TAG_VALUE_CACHE = "tagValueCache";

    @Inject
    public DefaultTagValueCache(final CacheMetricsProvider cacheMetricsProvider,
                                final TagValueCacheConfiguration config) {
        super(cacheMetricsProvider, config, TAG_VALUE_CACHE);
    }
}
