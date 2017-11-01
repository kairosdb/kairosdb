package org.kairosdb.datastore.cassandra.cache;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

import static com.google.inject.Scopes.SINGLETON;
import static org.kairosdb.datastore.cassandra.cache.DefaultMetricNameCache.METRIC_NAME_CACHE;
import static org.kairosdb.datastore.cassandra.cache.DefaultRowKeyCache.ROW_KEY_CACHE;

public class CachingModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(RedisConfiguration.class).in(SINGLETON);

        bind(RowKeyCacheConfiguration.class).asEagerSingleton();;

        bind(GeneralByteBufferCacheStore.class).annotatedWith(Names.named(ROW_KEY_CACHE))
                .to(RedisWriteBackReadThroughCacheStore.class).in(SINGLETON);
        bind(RowKeyCache.class).to(DefaultRowKeyCache.class).in(SINGLETON);

        bind(GeneralByteBufferCacheStore.class).annotatedWith(Names.named(METRIC_NAME_CACHE))
                .to(RedisWriteBackReadThroughCacheStore.class).in(SINGLETON);
        bind(MetricNameCache.class).to(DefaultMetricNameCache.class).in(SINGLETON);
    }
}
