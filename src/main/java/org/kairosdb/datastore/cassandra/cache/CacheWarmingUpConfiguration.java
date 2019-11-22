package org.kairosdb.datastore.cassandra.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class CacheWarmingUpConfiguration {
    public static final Logger logger = LoggerFactory.getLogger(CacheWarmingUpConfiguration.class);

    private final AtomicBoolean enabled = new AtomicBoolean(false);
    private final AtomicInteger heatingIntervalMinutes = new AtomicInteger(120);
    private final AtomicInteger rowIntervalInMinutes = new AtomicInteger(2);
    private final AtomicBoolean useRPS = new AtomicBoolean(false);
    private final AtomicInteger warmingUpInsertsPerSecond = new AtomicInteger(1000);

    public boolean isEnabled() {
        return enabled.get();
    }

    public int getHeatingIntervalMinutes() {
        return heatingIntervalMinutes.get();
    }

    public int getRowIntervalMinutes() {
        return rowIntervalInMinutes.get();
    }

    public boolean isUseRPS() {
        return useRPS.get();
    }

    public int getWarmingUpInsertsPerSecond() {
        return warmingUpInsertsPerSecond.get();
    }

    public void setEnabled(boolean enabled) {
        this.enabled.set(enabled);
    }

    public void setHeatingIntervalMinutes(final int newHeatingIntervalMinutes) {
        if (newHeatingIntervalMinutes < 1) {
            logger.warn(String.format("Discarding setting heatingIntervalMinutes to '%d' since it doesn't make sense", newHeatingIntervalMinutes));
            return;
        }
        this.heatingIntervalMinutes.set(newHeatingIntervalMinutes);
    }

    public void setRowIntervalInMinutes(int newRowIntervalInMinutes) {
        if (newRowIntervalInMinutes < 1) {
            logger.warn(String.format("Discarding setting rowIntervalInMinutes to '%d' since it doesn't make sense", newRowIntervalInMinutes));
            return;
        }
        this.rowIntervalInMinutes.set(newRowIntervalInMinutes);
    }

    public void setUseRPS(boolean enabled) {
        this.enabled.set(enabled);
    }

    public void setWarmingUpInsertsPerSecond(int newWarmingUpInsertsPerSecond) {
        if (newWarmingUpInsertsPerSecond < 1) {
            logger.warn(String.format("Discarding setting newWarmingUpInsertsPerSecond to '%d' since it doesn't make sense", newWarmingUpInsertsPerSecond));
            return;
        }
        this.rowIntervalInMinutes.set(newWarmingUpInsertsPerSecond);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CacheWarmingUpConfiguration{");
        sb.append("heatingIntervalMinutes=").append(heatingIntervalMinutes.get());
        sb.append(", enabled=").append(enabled.get());
        sb.append('}');
        return sb.toString();
    }
}
