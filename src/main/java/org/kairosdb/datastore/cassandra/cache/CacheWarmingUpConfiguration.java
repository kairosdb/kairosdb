package org.kairosdb.datastore.cassandra.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class CacheWarmingUpConfiguration {
    public static final Logger logger = LoggerFactory.getLogger(CacheWarmingUpConfiguration.class);

    private AtomicInteger heatingIntervalMinutes = new AtomicInteger(120);
    private AtomicBoolean enabled = new AtomicBoolean(false);

    public int getHeatingIntervalMinutes() {
        return heatingIntervalMinutes.get();
    }

    public boolean isEnabled() {
        return enabled.get();
    }

    public void setHeatingIntervalMinutes(final int newHeatingIntervalMinutes) {
        if (newHeatingIntervalMinutes < 5) {
            logger.warn(String.format("Discarding setting heatingIntervalMinutes to '%d' since it doesn't make sense", newHeatingIntervalMinutes));
        }
        this.heatingIntervalMinutes.set(newHeatingIntervalMinutes);
    }

    public void setEnabled(boolean enabled) {
        this.enabled.set(enabled);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CacheWarmingUpConfiguration{");
        sb.append("heatingIntervalMinutes=").append(heatingIntervalMinutes.get());
        sb.append('}');
        return sb.toString();
    }
}
