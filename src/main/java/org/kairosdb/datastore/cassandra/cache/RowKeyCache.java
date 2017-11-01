package org.kairosdb.datastore.cassandra.cache;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;

public interface RowKeyCache {
    void put(@Nonnull ByteBuffer rowKey);
    boolean isKnown(@Nonnull ByteBuffer rowKey);
}
