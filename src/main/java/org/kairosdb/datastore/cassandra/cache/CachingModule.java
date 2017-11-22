package org.kairosdb.datastore.cassandra.cache;

import com.google.inject.AbstractModule;
import org.kairosdb.datastore.cassandra.cache.persistence.*;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.name.Names.named;
import static org.kairosdb.datastore.cassandra.cache.DefaultRowKeyCache.ROW_KEY_CACHE;

public class CachingModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(ElastiCacheConfiguration.class).in(SINGLETON);

        bind(RowKeyCacheConfiguration.class).in(SINGLETON);
        bind(GeneralHashCacheStore.class).annotatedWith(named(ROW_KEY_CACHE))
                .to(ElastiCacheWriteBackReadThroughCacheStore.class).in(SINGLETON);
        bind(RowKeyCache.class).to(DefaultRowKeyCache.class).in(SINGLETON);
    }
}
