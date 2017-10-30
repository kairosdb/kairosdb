package org.kairosdb.datastore.cassandra.cache;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.Set;

public interface RowKeyCache {
    void put(@Nonnull ByteBuffer rowKey);
    void putAll(@Nonnull Set<ByteBuffer> rowKeys);
    boolean isKnown(@Nonnull ByteBuffer rowKey);
}
