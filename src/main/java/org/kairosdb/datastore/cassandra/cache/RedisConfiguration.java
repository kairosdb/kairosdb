package org.kairosdb.datastore.cassandra.cache;

import com.google.inject.Inject;
import com.google.inject.name.Named;

public class RedisConfiguration {
    private static final String HOSTNAME = "kairosdb.datastore.cassandra.cache.redis.hostname";
    private static final String PORT = "kairosdb.datastore.cassandra.cache.redis.port";
    private static final String WRITER_THREADS = "kairosdb.datastore.cassandra.cache.redis.writer_threads";
    private static final String WRITER_THREAD_IDLE_TIMEOUT_SECONDS = "kairosdb.datastore.cassandra.cache.redis.writer_thread_idle_timeout_seconds";

    @Inject(optional=true)
    @Named(HOSTNAME)
    private String hostName  = "localhost";

    @Inject(optional=true)
    @Named(PORT)
    private int port = 6379;

    @Inject(optional = true)
    @Named(WRITER_THREADS)
    private int writerThreads = 20;

    @Inject(optional = true)
    @Named(WRITER_THREAD_IDLE_TIMEOUT_SECONDS)
    private int workerThreadIdleTimeoutSeconds = 300;

    public String getHostName() {
        return hostName;
    }

    public int getPort() {
        return port;
    }

    public int getWriterThreads() {
        return writerThreads;
    }

    public int getWorkerThreadIdleTimeoutSeconds() {
        return workerThreadIdleTimeoutSeconds;
    }
}
