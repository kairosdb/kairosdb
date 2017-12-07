package org.kairosdb.core.http.rest.metrics;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.kairosdb.core.admin.InternalMetricsProvider;
import org.kairosdb.core.datastore.QueryMetric;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class DefaultQueryMeasurementProvider implements QueryMeasurementProvider, InternalMetricsProvider {
    private static final String MEASURES_PREFIX = "kairosdb.queries";

    private final MetricRegistry metricRegistry;
    private final Histogram spanHistogram;
    private final Histogram distanceHistogram;

    @Inject
    public DefaultQueryMeasurementProvider(@Nonnull final MetricRegistry metricRegistry) {
        checkNotNull(metricRegistry, "metricRegistry can't be null");
        this.metricRegistry = metricRegistry;


        spanHistogram = metricRegistry.histogram(MEASURES_PREFIX + ".span");
        distanceHistogram = metricRegistry.histogram(MEASURES_PREFIX + ".distance");
    }


    @Override
    public void measureSpan(final QueryMetric query) {
        long endTime = query.getEndTime();
        if(endTime == Long.MAX_VALUE) {
            final DateTime nowUTC = new DateTime(DateTimeZone.UTC);
            endTime = nowUTC.getMillis();
        }
        final long spanInMillis = endTime - query.getStartTime();
        final long spanInMinutes = spanInMillis / 1000 / 60;
        spanHistogram.update(spanInMinutes);
    }

    @Override
    public void measureDistance(final QueryMetric query) {
        final DateTime nowUTC = new DateTime(DateTimeZone.UTC);
        final long distanceInMillis = nowUTC.getMillis() - query.getStartTime();
        final long distanceInMinutes = distanceInMillis / 1000 / 60;
        distanceHistogram.update(distanceInMinutes);
    }

    @Override
    public Map<String, Metric> getAll() {
        final Map<String, Metric> cacheMetrics = metricRegistry.getMetrics().entrySet().stream()
                .filter(e -> e.getKey() != null && e.getKey().startsWith(MEASURES_PREFIX))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return ImmutableMap.copyOf(cacheMetrics);
    }

    @Override
    public Map<String, Metric> getForPrefix(@Nullable final String prefix) {
        if (prefix == null || prefix.isEmpty()) {
            return getAll();
        }

        final Map<String, Metric> filteredMetrics = getAll().entrySet().stream()
                .filter(e -> e.getKey() != null && e.getKey().startsWith(MEASURES_PREFIX + "." + prefix))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return ImmutableMap.copyOf(filteredMetrics);
    }
}
