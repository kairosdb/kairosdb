package org.kairosdb.util;

import ch.qos.logback.classic.Logger;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.codahale.metrics.jvm.*;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.KairosDBService;
import org.kairosdb.core.exception.KairosDBException;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;

/**
 * Created by irinaguberman on 12/8/14.
 */
public class InternalMetricsServer implements KairosDBService {

    public static final String GRAPHITE_HOST = "kairosdb.internal.graphite.host";
    public static final String GRAPHITE_PORT = "kairosdb.internal.graphite.port";
    public static final String GRAPHITE_INTERVAL = "kairosdb.internal.graphite.interval";
    public static final String GRAPHITE_TIME_UNIT = "kairosdb.internal.graphite.timeUnit";

    public static final String METRIC_GC = "kairosdb.internal.metric.gc";
    public static final String METRIC_BP = "kairosdb.internal.metric.bufferPool";
    public static final String METRIC_CACHED_TS = "kairosdb.internal.metric.cachedThreadStates";
    public static final String METRIC_FD_RATIO = "kairosdb.internal.metric.fdRatio";
    public static final String METRIC_MEM_USAGE = "kairosdb.internal.metric.memUsage";
    public static final String METRIC_CL = "kairosdb.internal.metric.classLoading";
    public static final String METRIC_TS = "kairosdb.internal.metric.threadStates";

    private Graphite graphite;
    private GraphiteReporter reporter;

    public static final Logger logger = (Logger) LoggerFactory.getLogger(InternalMetricsServer.class);

    @Inject
    public InternalMetricsServer(@Named(GRAPHITE_HOST) String hostname,
                                 @Named(GRAPHITE_PORT) int port,
                                 @Named(GRAPHITE_INTERVAL) int interval,
                                 @Named(GRAPHITE_TIME_UNIT) TimeUnit timeUnit,
                                 @Named(METRIC_GC) boolean gc,
                                 @Named(METRIC_BP) boolean bp,
                                 @Named(METRIC_CACHED_TS) boolean cachedTS,
                                 @Named(METRIC_FD_RATIO) boolean fdRatio,
                                 @Named(METRIC_MEM_USAGE) boolean memUsage,
                                 @Named(METRIC_CL) boolean cl,
                                 @Named(METRIC_TS) boolean ts){
        logger.warn("Constructing InternalMetricsServer with ".concat(hostname));
        if(hostname != null && hostname.length() > 0) {
            graphite = new Graphite(hostname, port);
            init_internal_metrics(interval, timeUnit,
                    gc, bp, cachedTS, fdRatio, memUsage, cl, ts);
        }
        else{
           logger.info("NOT starting internal metrics Graphite host is not set!");
        }
    }


    private void init_internal_metrics(Integer interval, TimeUnit timeUnit,
                                       boolean gc,
                                       boolean bp,
                                       boolean cachedTS,
                                       boolean fdRatio,
                                       boolean memUsage,
                                       boolean cl,
                                       boolean ts){
        logger.info("Starting internal metrics with interval {} and timeUnit {}", interval, timeUnit);

        com.codahale.metrics.MetricRegistry metrics = new MetricRegistry();

        if (bp) {
            metrics.register("buffer_pool", new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer()));
        }
        else{
            logger.info("Buffer_pool is OFF!!!");
        }

        if(cachedTS) {
            metrics.register("cached_thread_states", new CachedThreadStatesGaugeSet(interval, timeUnit));
        }
        if(cl){
            metrics.register("class_loading", new ClassLoadingGaugeSet());
        }

        if(fdRatio){
            metrics.register("file_descriptor_ratio", new FileDescriptorRatioGauge());
        }
        if(gc){
            metrics.register("GC", new GarbageCollectorMetricSet());
        }
        if(memUsage){
            metrics.register("mem_usage", new MemoryUsageGaugeSet());
        }
        if(ts){
            metrics.register("thread_states", new ThreadStatesGaugeSet());
        }

        reporter = GraphiteReporter.forRegistry(metrics)
                .prefixedWith("ubic")
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(MetricFilter.ALL)
                .build(graphite);
    }

    @Override
    public void start() throws KairosDBException {
        reporter.start(1, TimeUnit.MINUTES);
    }

    @Override
    public void stop() {
        reporter.stop();
    }
}
