package org.kairosdb.core.http.rest.metrics;

import org.kairosdb.core.admin.InternalMetricsProvider;
import org.kairosdb.core.datastore.QueryMetric;

public interface QueryMeasurementProvider extends InternalMetricsProvider {
    void measureSpan(QueryMetric query);
    void measureDistance(QueryMetric query);
}
