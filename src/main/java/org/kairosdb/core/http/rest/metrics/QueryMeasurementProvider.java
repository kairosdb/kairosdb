package org.kairosdb.core.http.rest.metrics;

import org.kairosdb.core.admin.InternalMetricsProvider;
import org.kairosdb.core.datastore.QueryMetric;

public interface QueryMeasurementProvider extends InternalMetricsProvider {
    void measureSpanSuccess(QueryMetric query);

    void measureDistanceSuccess(QueryMetric query);

    void measureSpanError(QueryMetric query);

    void measureDistanceError(QueryMetric query);

}
