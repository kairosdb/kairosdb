package org.kairosdb.datastore.cassandra.cache.persistence;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigInteger;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class RedisWriteBackReadThroughCacheStore implements GeneralHashCacheStore {
    private final Logger LOG = LoggerFactory.getLogger(RedisWriteBackReadThroughCacheStore.class);
    private final JedisPool jedisPool;
    private final ExecutorService executor;
    private final int defaultTtlInSeconds;

    @Inject
    public RedisWriteBackReadThroughCacheStore(final RedisConfiguration config) {
        this.defaultTtlInSeconds = config.getTtlInSeconds();
        this.jedisPool = createJedisPool(config);
        this.executor = createExecutorService(config);
    }

    private JedisPool createJedisPool(final RedisConfiguration config) {
        final URI uri = URI.create(String.format("redis://%s:%d", config.getHostName(), config.getPort()));
        final JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxWaitMillis(config.getConnectionTimeoutInMillis() + config.getSocketTimeoutInMillis());
        return new JedisPool(jedisPoolConfig, uri, config.getConnectionTimeoutInMillis(),
                config.getSocketTimeoutInMillis());
    }

    private ExecutorService createExecutorService(final RedisConfiguration config) {
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

    @CheckForNull
    @Override
    public Object load(@Nonnull final BigInteger key) throws Exception {
        checkNotNull(key, "cache key can't be null");
        try (final Jedis jedis = jedisPool.getResource()) {
            return jedis.get(key.toString());
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
            try (final Jedis jedis = jedisPool.getResource()) {
                jedis.setex(key.toString(), defaultTtlInSeconds, value.toString());
            } catch (Exception e) {
                LOG.error("failed to write back cache value for key {}", key, e);
            }
        });
    }

    @Override
    public void delete(@Nonnull final BigInteger key, @Nullable final Object value, @Nonnull final RemovalCause removalCause) {
        checkNotNull(key, "cache key can't be null");
        executor.submit(() -> {
            try (final Jedis jedis = jedisPool.getResource()) {
                jedis.del(key.toString());
            } catch (Exception e) {
                LOG.error("failed to delete cache key {}", key, e);
            }
        });
    }
}
