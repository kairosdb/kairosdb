package org.kairosdb.datastore.cassandra.cache;

import org.kairosdb.core.admin.CacheMetricsProvider;
import org.kairosdb.datastore.cassandra.cache.persistence.GeneralHashCacheStore;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;

import static org.mockito.Mockito.mock;

@Fork(2)
public class AbstractByteBufferCacheBenchmark {
    @Benchmark
    @BenchmarkMode(Mode.All)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 5, time = 10)
    public void measureDoubleHash() {
        final AbstractByteBufferCache cache = new AbstractByteBufferCache(mock(GeneralHashCacheStore.class),
                mock(CacheMetricsProvider.class), 42, 42, "foo", mock(Executor.class)) {};
        final byte[] KEY_PREFIX = "rowKeyCache:".getBytes();
        final ByteBuffer rowKey = ByteBuffer.wrap(new byte[]{42, 69, 1, 2, 3});
        final ByteBuffer prefixed = ByteBuffer.allocate(KEY_PREFIX.length + rowKey.limit()).put(KEY_PREFIX).put(rowKey);
        cache.doubleHash(prefixed);
    }
}