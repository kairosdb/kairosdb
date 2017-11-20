package org.kairosdb.core.admin;

import com.codahale.metrics.Metric;
import com.github.benmanes.caffeine.cache.Cache;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

public interface CacheMetricsProvider {
    /**
     * Returns the entire Map of Metrics.
     */
    Map<String, Metric> getAll();

    /**
     * Returns a Map of Metrics where the keys start with the given prefix.
     */
    Map<String, Metric> getForPrefix(@Nullable final String prefix);

    /**
     * Registers a a new cache in the metrics registry.
     *
     * @param cacheName the unique cache name that will be used as part of the metrics key
     * @param cache     the cache instance that provides the statistics
     */
    void registerCache(@Nonnull String cacheName, @Nonnull final Cache<?, ?> cache);
}
