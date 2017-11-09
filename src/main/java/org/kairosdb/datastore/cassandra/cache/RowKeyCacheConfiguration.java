package org.kairosdb.datastore.cassandra.cache;

import com.google.inject.Inject;
import com.google.inject.name.Named;

public class RowKeyCacheConfiguration implements CacheConfiguration {
    private static final String PREFIX = "kairosdb.datastore.cassandra.cache.row_key";
    private static final String TTL_IN_SECONDS = PREFIX + ".ttl_in_seconds";
    private static final String SIZE = PREFIX + ".size";

    @Inject(optional = true)
    @Named(TTL_IN_SECONDS)
    private int ttlInSeconds = 86_400; // defaults to 1 day

    @Inject(optional = true)
    @Named(SIZE)
    private int maxSize = 16_000_000;

    public int getTtlInSeconds() {
        return ttlInSeconds;
    }

    public int getMaxSize() {
        return maxSize;
    }
}
