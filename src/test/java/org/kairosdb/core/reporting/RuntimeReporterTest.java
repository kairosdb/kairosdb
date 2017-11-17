package org.kairosdb.core.reporting;

import org.junit.Test;
import org.kairosdb.core.DataPointSet;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RuntimeReporterTest {

    @Test
    public void testGetMetricsReturnsNecessaryMetrics() {
        RuntimeReporter reporter = new RuntimeReporter("foo");

        List<DataPointSet> points = reporter.getMetrics(0);
        assertEquals(4, points.size());

        assertTrue(points.stream().anyMatch(item -> "kairosdb.jvm.free_memory".equals(item.getName())));
        assertTrue(points.stream().anyMatch(item -> "kairosdb.jvm.total_memory".equals(item.getName())));
        assertTrue(points.stream().anyMatch(item -> "kairosdb.jvm.max_memory".equals(item.getName())));
        assertTrue(points.stream().anyMatch(item -> "kairosdb.jvm.thread_count".equals(item.getName())));
    }
}
