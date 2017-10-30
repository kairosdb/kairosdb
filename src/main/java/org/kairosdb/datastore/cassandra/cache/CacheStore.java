package org.kairosdb.datastore.cassandra.cache;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.CacheWriter;

public interface CacheStore<K, V> extends CacheWriter<K, V>, CacheLoader<K, V> {

}
