package org.kairosdb.datastore.cassandra.cache;

import com.google.inject.AbstractModule;
import org.kairosdb.datastore.cassandra.cache.persistence.*;

import java.util.concurrent.Executor;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.name.Names.named;
import static org.kairosdb.datastore.cassandra.cache.AsyncCacheExecutor.CACHE_EXECUTOR;
import static org.kairosdb.datastore.cassandra.cache.DefaultMetricNameCache.METRIC_NAME_CACHE;
import static org.kairosdb.datastore.cassandra.cache.DefaultRowKeyCache.ROW_KEY_CACHE;
import static org.kairosdb.datastore.cassandra.cache.DefaultTagNameCache.TAG_NAME_CACHE;
import static org.kairosdb.datastore.cassandra.cache.DefaultTagValueCache.TAG_VALUE_CACHE;

public class CachingModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(AsyncCacheExecutorConfiguration.class).in(SINGLETON);
        bind(AsyncCacheExecutor.class).in(SINGLETON);
        bind(ElastiCacheConfiguration.class).in(SINGLETON);
        bind(Executor.class).annotatedWith(named(CACHE_EXECUTOR)).to(AsyncCacheExecutor.class).in(SINGLETON);

        bind(RowKeyCacheConfiguration.class).in(SINGLETON);
        bind(GeneralHashCacheStore.class).annotatedWith(named(ROW_KEY_CACHE))
                .to(ElastiCacheWriteBackReadThroughCacheStore.class).in(SINGLETON);
        bind(RowKeyCache.class).to(DefaultRowKeyCache.class).in(SINGLETON);

        bind(AsyncCacheExecutorConfiguration.class).in(SINGLETON);
        bind(Executor.class).annotatedWith(named(ROW_KEY_CACHE)).to(AsyncCacheExecutor.class).in(SINGLETON);

        bindStringCache(METRIC_NAME_CACHE, CACHE_EXECUTOR, MetricNameCacheConfiguration.class, DefaultMetricNameCache.class);
        bindStringCache(TAG_NAME_CACHE, CACHE_EXECUTOR, TagNameCacheConfiguration.class, DefaultTagNameCache.class);
        bindStringCache(TAG_VALUE_CACHE, CACHE_EXECUTOR, TagValueCacheConfiguration.class, DefaultTagValueCache.class);
    }

    private void bindStringCache(final String cacheName,
                                 final String cacheExecutorName,
                                 final Class<? extends CacheConfiguration> configClass,
                                 final Class<? extends StringKeyCache> cacheClass) {
        bind(configClass).in(SINGLETON);
        bind(GeneralHashCacheStore.class).annotatedWith(named(cacheName))
                .to(ElastiCacheWriteBackReadThroughCacheStore.class).in(SINGLETON);
        bind(StringKeyCache.class).annotatedWith(named(cacheName)).to(cacheClass).in(SINGLETON);
    }
}
