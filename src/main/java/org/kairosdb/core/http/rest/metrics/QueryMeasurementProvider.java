package org.kairosdb.core.http.rest.metrics;

import org.kairosdb.core.admin.InternalMetricsProvider;
import org.kairosdb.core.datastore.QueryMetric;

import java.util.concurrent.ThreadPoolExecutor;

public interface QueryMeasurementProvider extends InternalMetricsProvider {
    void measureSpanSuccess(QueryMetric query);

    void measureDistanceSuccess(QueryMetric query);

    void measureSpanError(QueryMetric query);

    void measureDistanceError(QueryMetric query);

    void measureSpanForMetric(QueryMetric query);

    void measureDistanceForMetric(QueryMetric query);
}
