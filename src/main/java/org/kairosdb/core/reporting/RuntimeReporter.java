package org.kairosdb.core.reporting;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;

import java.util.List;

public class RuntimeReporter implements KairosMetricReporter {
    private final String hostname;
    private final Runtime runtime;

    @Inject
    private LongDataPointFactory dataPointFactory = new LongDataPointFactoryImpl();

    @Inject
    public RuntimeReporter(@Named("HOSTNAME") final String hostname) {
        this.hostname = hostname;
        this.runtime = Runtime.getRuntime();
    }

    private long getThreadCount() {
        ThreadGroup tg = Thread.currentThread().getThreadGroup();
        while (tg.getParent() != null) {
            tg = tg.getParent();
        }

        return tg.activeCount();
    }

    @Override
    public List<DataPointSet> getMetrics(final long now) {
        return ImmutableList.of(
                createDataPointSet("kairosdb.jvm.free_memory", now, runtime.freeMemory()),
                createDataPointSet("kairosdb.jvm.total_memory", now, runtime.totalMemory()),
                createDataPointSet("kairosdb.jvm.max_memory", now, runtime.totalMemory()),
                createDataPointSet("kairosdb.jvm.thread_count", now, getThreadCount())
        );
    }

    private DataPointSet createDataPointSet(final String name, final long timestamp, final long value) {
        final DataPointSet dataPointSet = new DataPointSet(name);
        dataPointSet.addTag("host", hostname);
        dataPointSet.addDataPoint(dataPointFactory.createDataPoint(timestamp, value));
        return dataPointSet;
    }
}
