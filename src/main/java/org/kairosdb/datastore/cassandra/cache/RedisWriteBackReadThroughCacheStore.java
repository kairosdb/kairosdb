package org.kairosdb.datastore.cassandra.cache;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class RedisWriteBackReadThroughCacheStore implements GeneralByteBufferCacheStore {
    private final Logger LOG = LoggerFactory.getLogger(RedisWriteBackReadThroughCacheStore.class);

    private final JedisPool jedisPool;
    private final ExecutorService executor;
    private final int defaultTtlInSeconds;

    @Inject
    public RedisWriteBackReadThroughCacheStore(final RedisConfiguration redisConfiguration,
                                               final RowKeyCacheConfiguration rowKeyCacheConfiguration) {
        this.jedisPool = new JedisPool(redisConfiguration.getHostName(), redisConfiguration.getPort());
        this.defaultTtlInSeconds = rowKeyCacheConfiguration.getDefaultTtlInSeconds();
        final ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
                redisConfiguration.getWriterThreads(),
                redisConfiguration.getWriterThreads(),
                redisConfiguration.getWorkerThreadIdleTimeoutSeconds(),
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                new ThreadFactoryBuilder().setNameFormat(RedisWriteBackReadThroughCacheStore.class.getSimpleName() +
                        "-writer-thread-%d").build());
        threadPoolExecutor.allowCoreThreadTimeOut(true);
        this.executor = threadPoolExecutor;
    }

    @CheckForNull
    @Override
    public Boolean load(@Nonnull final ByteBuffer key) throws Exception {
        checkNotNull(key, "cache key can't be null");
        try (final Jedis jedis = jedisPool.getResource()) {
            final byte[] bytes = jedis.get(key.array());
            return Boolean.valueOf(new String(bytes));
        } catch (Exception e) {
            LOG.error("failed to load cache value for key {}", key, e);
            throw e;
        }
    }

    @Override
    public void write(@Nonnull final ByteBuffer key, @Nonnull final Boolean value) {
        checkNotNull(key, "cache key can't be null");
        checkNotNull(value, "cache value can't be null");
        executor.submit(() -> {
            try (final Jedis jedis = jedisPool.getResource()) {
                jedis.setex(key.array(), defaultTtlInSeconds, value.toString().getBytes());
            } catch (Exception e) {
                LOG.error("failed to write back cache value for key {}", key, e);
            }
        });
    }

    @Override
    public void delete(@Nonnull final ByteBuffer key, @Nullable final Boolean value, @Nonnull final RemovalCause removalCause) {
        checkNotNull(key, "cache key can't be null");
        executor.submit(() -> {
            try (final Jedis jedis = jedisPool.getResource()) {
                jedis.del(key.array());
            } catch (Exception e) {
                LOG.error("failed to delete cache key {}", key, e);
            }
        });
    }
}
