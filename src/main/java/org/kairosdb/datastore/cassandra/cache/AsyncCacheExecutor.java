package org.kairosdb.datastore.cassandra.cache;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import org.kairosdb.datastore.cassandra.cache.persistence.RedisWriteBackReadThroughCacheStore;

import java.util.concurrent.*;

public class AsyncCacheExecutor implements Executor {
    public static final String CACHE_EXECUTOR = "cacheExecutor";

    private Executor executor;

    @Inject
    public AsyncCacheExecutor(final AsyncCacheExecutorConfiguration configuration) {
        this.executor = createExecutorService(configuration);
    }

    private ExecutorService createExecutorService(final AsyncCacheExecutorConfiguration config) {
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
