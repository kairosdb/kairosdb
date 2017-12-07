package org.kairosdb.core.admin;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

public class CacheMetricsModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(CacheMetricsProvider.class).to(DefaultCacheMetricsProvider.class).in(Singleton.class);
    }
}
