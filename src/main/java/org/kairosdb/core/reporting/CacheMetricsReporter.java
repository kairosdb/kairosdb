package org.kairosdb.core.reporting;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.admin.CacheMetricsProvider;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.datapoints.DoubleDataPointFactoryImpl;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.kairosdb.core.reporting.MetricReporterService.HOSTNAME;

public class CacheMetricsReporter implements KairosMetricReporter {
    private final CacheMetricsProvider cacheMetricsProvider;

    @Inject
    private LongDataPointFactory longDataPointFactory = new LongDataPointFactoryImpl();
    @Inject
    private DoubleDataPointFactory doubleDataPointFactory = new DoubleDataPointFactoryImpl();

    private String hostname;

    @Inject
    public CacheMetricsReporter(final CacheMetricsProvider cacheMetricsProvider, @Named(HOSTNAME) final String hostname) {
        checkNotNull(cacheMetricsProvider, "cacheMetricsProvider can't be null");
        checkNotNull(hostname, "hostname can't be null");
        this.cacheMetricsProvider = cacheMetricsProvider;
        this.hostname = hostname;
    }

    @Override
    public List<DataPointSet> getMetrics(final long now) {
        final ImmutableList.Builder<DataPointSet> builder = ImmutableList.builder();
        final Map<String, Metric> metrics = cacheMetricsProvider.getAll();
        for (final Map.Entry<String, Metric> metric: metrics.entrySet()) {
            if(metric.getValue() instanceof Gauge) {
                final Gauge gauge = (Gauge)metric.getValue();
                if(gauge.getValue() instanceof Number) {
                    final Number number = (Number)gauge.getValue();
                    builder.add(createDataPointSet(metric.getKey(), now, number));
                }
            }
        }
        return builder.build();
    }

    private DataPointSet createDataPointSet(final String name, final long timestamp, final Number value) {
        final DataPointSet dataPointSet = new DataPointSet(name);
        final DataPoint dataPoint;
        if(value instanceof Double || value instanceof Float) {
            dataPoint = doubleDataPointFactory.createDataPoint(timestamp, value.doubleValue());
        } else {
            dataPoint = longDataPointFactory.createDataPoint(timestamp, value.longValue());
        }
        dataPointSet.addDataPoint(dataPoint);
        dataPointSet.addTag("entity", hostname);
        return dataPointSet;
    }

}
