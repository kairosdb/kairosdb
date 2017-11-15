package org.kairosdb.core.admin;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.AbstractModule;

import javax.inject.Singleton;

public class CacheMetricsModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(MetricRegistry.class).in(Singleton.class);
        bind(CacheMetricsProvider.class).to(DefaultCacheMetricsProvider.class).in(Singleton.class);
    }
}
