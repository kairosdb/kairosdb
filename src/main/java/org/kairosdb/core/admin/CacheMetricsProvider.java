package org.kairosdb.core.admin;

import com.github.benmanes.caffeine.cache.Cache;

import javax.annotation.Nonnull;

public interface CacheMetricsProvider extends InternalMetricsProvider {
    /**
     * Registers a a new cache in the metrics registry.
     *
     * @param cacheName the unique cache name that will be used as part of the metrics key
     * @param cache     the cache instance that provides the statistics
     */
    void registerCache(@Nonnull String cacheName, @Nonnull final Cache<?, ?> cache);
}
