package org.kairosdb.datastore.cassandra.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class CacheWarmingUpConfiguration {
    public static final Logger logger = LoggerFactory.getLogger(CacheWarmingUpConfiguration.class);

    private AtomicInteger heatingIntervalMinutes = new AtomicInteger(30);

    public int getHeatingIntervalMinutes() {
        return heatingIntervalMinutes.get();
    }

    void update(final int newHeatingIntervalMinutes) {
        if (newHeatingIntervalMinutes < 5) {
            logger.warn(String.format("Discarding setting heatingIntervalMinutes to '%d' since it doesn't make sense", newHeatingIntervalMinutes));
        }
        this.heatingIntervalMinutes.set(newHeatingIntervalMinutes);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CacheWarmingUpConfiguration{");
        sb.append("heatingIntervalMinutes=").append(heatingIntervalMinutes.get());
        sb.append('}');
        return sb.toString();
    }
}
