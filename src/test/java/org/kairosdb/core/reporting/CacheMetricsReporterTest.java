package org.kairosdb.core.reporting;

import com.codahale.metrics.*;
import org.junit.Test;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.admin.CacheMetricsProvider;

import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CacheMetricsReporterTest {

    @Test
    public void testGetMetricsExtractsNumericsFromGaugesOnly() {
        CacheMetricsProvider cacheMetricsProvider = mock(CacheMetricsProvider.class);
        Gauge gauge = () -> 42;

        when(cacheMetricsProvider.getAll()).thenReturn(new HashMap<String, Metric>() {
            {
                put("foo", mock(Counter.class));
                put("bar", gauge);
                put("baz", gauge);
                put("boo", mock(Histogram.class));
            }
        });

        CacheMetricsReporter reporter = new CacheMetricsReporter(cacheMetricsProvider, "localhost");

        List<DataPointSet> points = reporter.getMetrics(0);
        assertEquals(2, points.size());
        assertTrue(points.stream().anyMatch(item -> "bar".equals(item.getName())));
        assertTrue(points.stream().anyMatch(item -> "baz".equals(item.getName())));
    }
}
