package org.kairosdb.core.http.rest.metrics;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.Guice;
import io.opentracing.util.GlobalTracer;
import org.junit.Test;
import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.http.WebServletModule;

import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DefaultQueryMeasurementProviderTest {

    @Test
    public void testCreatesNecessaryHistograms() {
        final MetricRegistry registry = new MetricRegistry();
        final DefaultQueryMeasurementProvider provider = new DefaultQueryMeasurementProvider(registry, GlobalTracer.get());

        Set<String> names = registry.getNames();

        assertEquals(4, names.size());
        assertTrue(names.stream().anyMatch(item -> "kairosdb.queries.span.success".equals(item)));
        assertTrue(names.stream().anyMatch(item -> "kairosdb.queries.distance.success".equals(item)));
        assertTrue(names.stream().anyMatch(item -> "kairosdb.queries.span.error".equals(item)));
        assertTrue(names.stream().anyMatch(item -> "kairosdb.queries.distance.error".equals(item)));
    }

    @Test
    public void testIgnoresMetricsStartingWithPrefix() {
        final MetricRegistry registry = new MetricRegistry();
        final DefaultQueryMeasurementProvider provider = new DefaultQueryMeasurementProvider(registry, GlobalTracer.get());
        provider.measureSpanForMetric(new QueryMetric(0l, 0, "foo"));
        provider.measureSpanForMetric(new QueryMetric(0l, 0, "foo"));
        provider.measureSpanForMetric(new QueryMetric(0l, 0, "kairosdb.queries.foo"));

        Set<String> names = registry.getNames();

        assertEquals(5, names.size());
        assertTrue(names.stream().anyMatch(item -> "kairosdb.queries.span.success".equals(item)));
        assertTrue(names.stream().anyMatch(item -> "kairosdb.queries.distance.success".equals(item)));
        assertTrue(names.stream().anyMatch(item -> "kairosdb.queries.span.error".equals(item)));
        assertTrue(names.stream().anyMatch(item -> "kairosdb.queries.distance.error".equals(item)));
        assertTrue(names.stream().anyMatch(item -> "kairosdb.queries.foo.span".equals(item)));
    }

}
