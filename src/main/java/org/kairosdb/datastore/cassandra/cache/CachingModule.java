package org.kairosdb.datastore.cassandra.cache;

import com.google.inject.AbstractModule;

import static com.google.inject.Scopes.SINGLETON;

public class CachingModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(RowKeyCacheConfiguration.class).in(SINGLETON);

        bind(RedisConfiguration.class).in(SINGLETON);
        bind(GeneralByteBufferCacheStore.class).to(RedisWriteBackReadThroughCacheStore.class).in(SINGLETON);
        bind(RowKeyCache.class).to(WriteBackRowKeyCache.class).in(SINGLETON);
    }
}
