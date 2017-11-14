package org.kairosdb.core.admin;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.AbstractModule;

import javax.inject.Singleton;

public class MetricsModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(MetricRegistry.class).in(Singleton.class);
        bind(MetricsProvider.class).to(DefaultMetricsProvider.class).in(Singleton.class);
    }
}
