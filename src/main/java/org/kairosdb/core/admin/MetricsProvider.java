package org.kairosdb.core.admin;

import com.codahale.metrics.Metric;
import com.github.benmanes.caffeine.cache.Cache;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

public interface MetricsProvider {
    /**
     * Returns a Map of Metrics where the keys start with the given prefix.
     */
    Map<String, Metric> getForPrefix(@Nullable final String prefix);
    void registerCache(@Nonnull String cacheName, @Nonnull final Cache<?, ?> cache);
}
