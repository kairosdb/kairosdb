package org.kairosdb.datastore.cassandra.cache;

import com.google.inject.Inject;
import com.google.inject.name.Named;

public class AsyncCacheExecutorConfiguration {
    private static final String PREFIX = "kairosdb.datastore.cassandra.cache.executor.";

    private static final String WRITER_THREADS = PREFIX + "writer_threads";
    private static final String WRITER_THREAD_IDLE_TIMEOUT_SECONDS = PREFIX + "writer_thread_idle_timeout_seconds";
    private static final String MAX_QUEUE_SIZE = PREFIX + "max_queue_size";

    @Inject(optional = true)
    @Named(WRITER_THREADS)
    private int writerThreads = 150;

    @Inject(optional = true)
    @Named(WRITER_THREAD_IDLE_TIMEOUT_SECONDS)
    private int workerThreadIdleTimeoutSeconds = 300;

    @Inject(optional=true)
    @Named(MAX_QUEUE_SIZE)
    private int maxQueueSize = 250_000;

    public int getWriterThreads() {
        return writerThreads;
    }

    public int getWorkerThreadIdleTimeoutSeconds() {
        return workerThreadIdleTimeoutSeconds;
    }

    public int getMaxQueueSize() {
        return maxQueueSize;
    }

}
