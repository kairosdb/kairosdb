package org.kairosdb.datastore.cassandra.cache;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.datastore.cassandra.cache.persistence.GeneralHashCacheStore;

public class DefaultMetricNameCache extends AbstractStringCache implements StringKeyCache {
    public static final String METRIC_NAME_CACHE = "metricNameCache";

    @Inject
    public DefaultMetricNameCache(@Named(METRIC_NAME_CACHE) final GeneralHashCacheStore cacheStore,
                                  final MetricNameCacheConfiguration config) {
        super(cacheStore, config);
    }
}
