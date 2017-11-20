package org.kairosdb.datastore.cassandra.cache.persistence;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import net.rubyeye.xmemcached.aws.AWSElasticCacheClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.*;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.util.concurrent.*;

import static com.google.common.base.Preconditions.checkNotNull;

public class ElastiCacheWriteBackReadThroughCacheStore implements GeneralHashCacheStore {
    private final Logger LOG = LoggerFactory.getLogger(ElastiCacheWriteBackReadThroughCacheStore.class);
    private final ExecutorService executor;
    private final int defaultTtlInSeconds;
    private final AWSElasticCacheClient client;

    @Inject
    public ElastiCacheWriteBackReadThroughCacheStore(final ElastiCacheConfiguration config) throws IOException {
        this.defaultTtlInSeconds = config.getTtlInSeconds();
        this.executor = createExecutorService(config);

        this.client = new AWSElasticCacheClient(new InetSocketAddress(config.getHostName(), config.getPort()));
        this.client.setConnectTimeout(config.getConnectionTimeoutInMillis());
        this.client.setOpTimeout(config.getOperationTimeoutInMillis());
    }

    private ExecutorService createExecutorService(final ElastiCacheConfiguration config) {
        final ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
                config.getWriterThreads(),
                config.getWriterThreads(),
                config.getWorkerThreadIdleTimeoutSeconds(), TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(config.getMaxQueueSize()),
                new ThreadFactoryBuilder().setNameFormat(ElastiCacheWriteBackReadThroughCacheStore.class.getSimpleName() +
                        "-writer-thread-%d").build());
        threadPoolExecutor.allowCoreThreadTimeOut(true);
        return threadPoolExecutor;
    }

    @CheckForNull
    @Override
    public Object load(@Nonnull final BigInteger key) throws Exception {
        checkNotNull(key, "cache key can't be null");
        try {
            return client.get(key.toString());
        } catch (Exception e) {
            LOG.error("failed to load cache value for key {}", key, e);
            return null;
        }
    }

    @Override
    public void write(@Nonnull final BigInteger key, @Nonnull final Object value) {
        checkNotNull(key, "cache key can't be null");
        checkNotNull(value, "cache value can't be null");
        executor.submit(() -> {
            try {
                client.set(key.toString(), defaultTtlInSeconds, value.toString());
            } catch (Exception e) {
                LOG.error("failed to write back cache value for key {}", key, e);
            }
        });
    }

    @Override
    public void delete(@Nonnull final BigInteger key, @Nullable final Object value, @Nonnull final RemovalCause removalCause) {
        checkNotNull(key, "cache key can't be null");
        executor.submit(() -> {
            try {
                client.delete(key.toString());
            } catch (Exception e) {
                LOG.error("failed to delete cache key {}", key, e);
            }
        });
    }
}
