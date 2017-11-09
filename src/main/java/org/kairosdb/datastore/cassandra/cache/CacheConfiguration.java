package org.kairosdb.datastore.cassandra.cache;

public interface CacheConfiguration {
    int getTtlInSeconds();
    int getMaxSize();
}
