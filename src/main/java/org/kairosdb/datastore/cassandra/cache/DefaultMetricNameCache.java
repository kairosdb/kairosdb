package org.kairosdb.datastore.cassandra.cache;

import com.google.inject.Inject;
import org.kairosdb.core.admin.CacheMetricsProvider;

public class DefaultMetricNameCache extends AbstractStringCache {
    public static final String METRIC_NAME_CACHE = "metricNameCache";

    @Inject
    public DefaultMetricNameCache(final CacheMetricsProvider cacheMetricsProvider,
                                  final MetricNameCacheConfiguration config) {
        super(cacheMetricsProvider, config, METRIC_NAME_CACHE);
    }
}
