package org.kairosdb.datastore.cassandra.cache;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.kairosdb.datastore.cassandra.cache.persistence.RedisWriteBackReadThroughCacheStore;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ExecutorConfigurer implements Executor {
    public static final String CACHE_EXECUTOR = "cacheExecutor";

    private Executor executor;

    public ExecutorConfigurer(final CacheExecutorConfiguration configuration) {
        this.executor = createExecutorService(configuration);
    }

    private ExecutorService createExecutorService(final CacheExecutorConfiguration config) {
        final ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
                config.getWriterThreads(),
                config.getWriterThreads(),
                config.getWorkerThreadIdleTimeoutSeconds(), TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(config.getMaxQueueSize()),
                new ThreadFactoryBuilder().setNameFormat(RedisWriteBackReadThroughCacheStore.class.getSimpleName() +
                        "-writer-thread-%d").build());
        threadPoolExecutor.allowCoreThreadTimeOut(true);
        return threadPoolExecutor;
    }


    @Override
    public void execute(final Runnable command) {
        this.executor.execute(command);
    }
}
