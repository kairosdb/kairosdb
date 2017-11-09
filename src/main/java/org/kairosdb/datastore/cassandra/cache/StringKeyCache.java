package org.kairosdb.datastore.cassandra.cache;

import javax.annotation.Nonnull;

public interface StringKeyCache {
    void put(@Nonnull String metricName);
    boolean isKnown(@Nonnull String metricName);
}
