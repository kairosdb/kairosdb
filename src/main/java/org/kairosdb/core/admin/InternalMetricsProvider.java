package org.kairosdb.core.admin;

import com.codahale.metrics.Metric;

import javax.annotation.Nullable;
import java.util.Map;

public interface InternalMetricsProvider {
    /**
     * Returns the entire Map of Metrics.
     */
    Map<String, Metric> getAll();

    /**
     * Filters the metrics from the metrics registry using the provided prefix. When the prefix is null, or empty,
     * an immediate copy of the metrics registry is returned.
     *
     * @param prefix a string that should match, case sensitive, with the keys from the metrics registry
     * @return An immutable copy of the metrics that start with the given prefix
     */
    Map<String, Metric> getForPrefix(@Nullable final String prefix);
}
