package org.kairosdb.core.admin;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.codahale.metrics.MetricRegistry.name;
import static com.google.common.base.CaseFormat.LOWER_HYPHEN;
import static com.google.common.base.CaseFormat.UPPER_UNDERSCORE;
import static com.google.common.base.Preconditions.checkNotNull;

public class DefaultCacheMetricsProvider implements CacheMetricsProvider {
    private static final String CACHE_PREFIX = "kairosdb.cache";

    enum CacheMetrics {
        REQUEST_COUNT,
        HIT_COUNT,
        HIT_RATE,
        MISS_COUNT,
        MISS_RATE,
        LOAD_COUNT,
        LOAD_SUCCESS_COUNT,
        LOAD_FAILURE_COUNT,
        LOAD_FAILURE_RATE,
        TOTAL_LOAD_TIME,
        AVG_LOAD_PENALTY,
        EVICTION_COUNT,
        SIZE
    }
    private static final Set<CacheMetrics> ALL_METRICS = EnumSet.allOf(CacheMetrics.class);

    private final MetricRegistry metricRegistry;

    @Inject
    public DefaultCacheMetricsProvider(@Nonnull final MetricRegistry metricRegistry) {
        checkNotNull(metricRegistry, "metricRegistry can't be null");
        this.metricRegistry = metricRegistry;
    }

    @Override
    public Map<String, Metric> getAll() {
        return ImmutableMap.copyOf(metricRegistry.getMetrics());
    }

    /**
     * Filters the metrics from the metrics registry using the provided prefix. When the prefix is null, or empty,
     * an immediate copy of the metrics registry is returned.
     *
     * @param prefix a string that should match, case sensitive, with the keys from the metrics registry
     * @return An immutable copy of the metrics that start with the given prefix
     */
    @Override
    public Map<String, Metric> getForPrefix(@Nullable final String prefix) {
        if (prefix == null || prefix.isEmpty()) {
            return getAll();
        }

        final Map<String, Metric> filteredMetrics = metricRegistry.getMetrics().entrySet().stream()
                .filter(e -> e.getKey() != null && e.getKey().startsWith(prefix))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return ImmutableMap.copyOf(filteredMetrics);
    }

    @Override
    public void registerCache(@Nonnull final String cacheName, @Nonnull final Cache<?, ?> cache) {
        checkNotNull(cacheName, "cacheName can't be null");
        checkNotNull(cache, "cache can't be null");

        for (final CacheMetrics metric : ALL_METRICS) {
            final String fqmn = name(CACHE_PREFIX, cacheName,
                    UPPER_UNDERSCORE.to(LOWER_HYPHEN, metric.name()));
            metricRegistry.register(fqmn, createGauge(metric, cache));
        }
    }

    private Gauge<Number> createGauge(final CacheMetrics metric, final Cache<?, ?> cache) {
        return () -> {
            final CacheStats stats = cache.stats();
            switch (metric) {
                case REQUEST_COUNT:
                    return stats.requestCount();
                case HIT_COUNT:
                    return stats.hitCount();
                case HIT_RATE:
                    return stats.hitRate();
                case MISS_COUNT:
                    return stats.missCount();
                case MISS_RATE:
                    return stats.missRate();
                case LOAD_COUNT:
                    return stats.loadCount();
                case LOAD_SUCCESS_COUNT:
                    return stats.loadSuccessCount();
                case LOAD_FAILURE_COUNT:
                    return stats.loadFailureCount();
                case LOAD_FAILURE_RATE:
                    return stats.loadFailureRate();
                case TOTAL_LOAD_TIME:
                    return stats.totalLoadTime();
                case AVG_LOAD_PENALTY:
                    return stats.averageLoadPenalty();
                case EVICTION_COUNT:
                    return stats.evictionCount();
                case SIZE:
                    return cache.estimatedSize();
            }
            throw new IllegalArgumentException("Invalid CacheMetric enum value:" + metric);
        };
    }
}
