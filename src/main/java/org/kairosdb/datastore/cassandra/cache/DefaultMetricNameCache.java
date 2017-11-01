package org.kairosdb.datastore.cassandra.cache;

import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;

import static org.slf4j.LoggerFactory.getLogger;

public class DefaultMetricNameCache extends AbstractByteBufferCache implements MetricNameCache {
    public static final String METRIC_NAME_CACHE = "metricNameCache";
    private static final Logger LOG = getLogger(DefaultMetricNameCache.class);
    private static Charset UTF8 = Charset.forName("UTF-8");

    public DefaultMetricNameCache(final GeneralByteBufferCacheStore cacheStore,
                                  final MetricNameCacheConfiguration config) {
        super(cacheStore, config.getMaxSize(), config.getTtlInSeconds());
    }

    @Override
    public void put(@Nonnull String metricName) {
        try {
            final ByteBuffer encoded = encodeName(metricName);
            this.outerLayerCache.put(encoded, Boolean.TRUE);
        } catch (Exception e) {
            LOG.error("failed to write cached metric name {}", metricName);
        }
    }

    @Override
    public boolean isKnown(@Nonnull String metricName) {
        try {
            final ByteBuffer encoded = encodeName(metricName);
            return this.outerLayerCache.get(encoded) != null;
        } catch (Exception e) {
            LOG.error("failed to read cached metric name {}", metricName);
        }
        return false;
    }

    private ByteBuffer encodeName(final String metricName) throws CharacterCodingException {
        final String prefixedKey = "cache.metric." + metricName;
        return UTF8.newEncoder().encode(CharBuffer.wrap(prefixedKey));
    }
}
