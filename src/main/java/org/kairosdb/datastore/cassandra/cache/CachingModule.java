package org.kairosdb.datastore.cassandra.cache;

import com.google.inject.AbstractModule;
import org.kairosdb.datastore.cassandra.cache.persistence.GeneralHashCacheStore;
import org.kairosdb.datastore.cassandra.cache.persistence.RedisConfiguration;
import org.kairosdb.datastore.cassandra.cache.persistence.RedisWriteBackReadThroughCacheStore;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.name.Names.named;
import static org.kairosdb.datastore.cassandra.cache.DefaultMetricNameCache.METRIC_NAME_CACHE;
import static org.kairosdb.datastore.cassandra.cache.DefaultRowKeyCache.ROW_KEY_CACHE;
import static org.kairosdb.datastore.cassandra.cache.DefaultTagNameCache.TAG_NAME_CACHE;
import static org.kairosdb.datastore.cassandra.cache.DefaultTagValueCache.TAG_VALUE_CACHE;

public class CachingModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(RedisConfiguration.class).in(SINGLETON);

        bind(RowKeyCacheConfiguration.class).in(SINGLETON);
        bind(GeneralHashCacheStore.class).annotatedWith(named(ROW_KEY_CACHE))
                .to(RedisWriteBackReadThroughCacheStore.class).in(SINGLETON);
        bind(RowKeyCache.class).to(DefaultRowKeyCache.class).in(SINGLETON);

        bindStringCache(METRIC_NAME_CACHE, MetricNameCacheConfiguration.class, DefaultMetricNameCache.class);
        bindStringCache(TAG_NAME_CACHE, TagNameCacheConfiguration.class, DefaultTagNameCache.class);
        bindStringCache(TAG_VALUE_CACHE, TagValueCacheConfiguration.class, DefaultTagValueCache.class);
    }

    private void bindStringCache(final String cacheName, final Class<? extends CacheConfiguration> configClass,
                                 final Class<? extends StringKeyCache> clazz) {
        bind(configClass).in(SINGLETON);
        bind(GeneralHashCacheStore.class).annotatedWith(named(cacheName))
                .to(RedisWriteBackReadThroughCacheStore.class).in(SINGLETON);
        bind(StringKeyCache.class).annotatedWith(named(cacheName)).to(clazz).in(SINGLETON);

    }
}
