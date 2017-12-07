package org.kairosdb.core.reporting;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Metric;
import com.codahale.metrics.Snapshot;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.datapoints.DoubleDataPointFactoryImpl;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;
import org.kairosdb.core.http.rest.metrics.QueryMeasurementProvider;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.kairosdb.core.reporting.MetricReporterService.HOSTNAME;

public class QueryMeasurementsReporter implements KairosMetricReporter {
    @Inject
    private LongDataPointFactory longDataPointFactory = new LongDataPointFactoryImpl();
    @Inject
    private DoubleDataPointFactory doubleDataPointFactory = new DoubleDataPointFactoryImpl();

    private final QueryMeasurementProvider queryMeasurementProvider;

    private String hostname;

    @Inject
    public QueryMeasurementsReporter(final QueryMeasurementProvider queryMeasurementProvider, @Named(HOSTNAME) final String hostname) {
        checkNotNull(queryMeasurementProvider, "queryMeasurementProvider can't be null");
        checkNotNull(hostname, "hostname can't be null");
        this.queryMeasurementProvider = queryMeasurementProvider;
        this.hostname = hostname;
    }

    @Override
    public List<DataPointSet> getMetrics(final long now) {
        final ImmutableList.Builder<DataPointSet> builder = ImmutableList.builder();
        final Map<String, Metric> metrics = queryMeasurementProvider.getAll();
        for (final Map.Entry<String, Metric> metric: metrics.entrySet()) {
            if(metric.getValue() instanceof Histogram) {
                final Histogram histogram = (Histogram) metric.getValue();
                final Snapshot snapshot = histogram.getSnapshot();
                final String metricName = metric.getKey();
                builder.add(createDataPointSet(metricName, "max", now, snapshot.getMax()));
                builder.add(createDataPointSet(metricName, "min", now, snapshot.getMin()));
                builder.add(createDataPointSet(metricName, "mean", now, snapshot.getMean()));
                builder.add(createDataPointSet(metricName, "median", now, snapshot.getMedian()));
                builder.add(createDataPointSet(metricName, "p75", now, snapshot.get75thPercentile()));
                builder.add(createDataPointSet(metricName, "p98", now, snapshot.get98thPercentile()));
                builder.add(createDataPointSet(metricName, "p99", now, snapshot.get99thPercentile()));
            }
        }
        return builder.build();
    }

    private DataPointSet createDataPointSet(final String metricName, final String key, final long timestamp,
                                            final Number value) {
        final DataPointSet dataPointSet = new DataPointSet(metricName);
        final DataPoint dataPoint = createDataPoint(timestamp, value);
        dataPointSet.addDataPoint(dataPoint);
        dataPointSet.addTag("key", key);
        dataPointSet.addTag("entity", hostname);
        return dataPointSet;
    }

    private DataPoint createDataPoint(final long timestamp, final Number value) {
        if(value instanceof Double || value instanceof Float) {
            return doubleDataPointFactory.createDataPoint(timestamp, value.doubleValue());
        } else {
            return longDataPointFactory.createDataPoint(timestamp, value.longValue());
        }

    }

}
