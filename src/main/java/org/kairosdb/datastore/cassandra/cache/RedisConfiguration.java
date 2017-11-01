package org.kairosdb.datastore.cassandra.cache;

import com.google.inject.Inject;
import com.google.inject.name.Named;

public class RedisConfiguration {
    private static final String PREFIX = "kairosdb.datastore.cassandra.cache.redis.";

    private static final String HOSTNAME = PREFIX + "hostname";
    private static final String PORT = PREFIX + "port";
    private static final String CONNECTION_TIMEOUT_IN_MILLIS = PREFIX + "connection_timeout_in_millis";
    private static final String SOCKET_TIMEOUT_MILLIS = PREFIX + "socket_timeout_in_millis";
    private static final String WRITER_THREADS = PREFIX + "writer_threads";
    private static final String WRITER_THREAD_IDLE_TIMEOUT_SECONDS = PREFIX + "writer_thread_idle_timeout_seconds";
    private static final String TTL_IN_SECONDS = PREFIX + "ttl_in_seconds";
    private static final String MAX_QUEUE_SIZE = PREFIX + "max_queue_size";

    @Inject(optional=true)
    @Named(HOSTNAME)
    private String hostName  = "localhost";

    @Inject(optional=true)
    @Named(PORT)
    private int port = 6379;

    @Inject(optional=true)
    @Named(CONNECTION_TIMEOUT_IN_MILLIS)
    private int connectionTimeoutInMillis = 500;

    @Inject(optional=true)
    @Named(SOCKET_TIMEOUT_MILLIS)
    private int socketTimeoutInMillis = 50;

    @Inject(optional = true)
    @Named(WRITER_THREADS)
    private int writerThreads = 20;

    @Inject(optional = true)
    @Named(WRITER_THREAD_IDLE_TIMEOUT_SECONDS)
    private int workerThreadIdleTimeoutSeconds = 300;

    @Inject(optional=true)
    @Named(TTL_IN_SECONDS)
    private int ttlInSeconds = 691_200; // 8 days default

    @Inject(optional=true)
    @Named(MAX_QUEUE_SIZE)
    private int maxQueueSize = 20_000;

    public String getHostName() {
        return hostName;
    }

    public int getPort() {
        return port;
    }

    public int getConnectionTimeoutInMillis() {
        return connectionTimeoutInMillis;
    }

    public int getSocketTimeoutInMillis() {
        return socketTimeoutInMillis;
    }

    public int getWriterThreads() {
        return writerThreads;
    }

    public int getWorkerThreadIdleTimeoutSeconds() {
        return workerThreadIdleTimeoutSeconds;
    }

    public int getTtlInSeconds() {
        return ttlInSeconds;
    }

    public int getMaxQueueSize() {
        return maxQueueSize;
    }
}
