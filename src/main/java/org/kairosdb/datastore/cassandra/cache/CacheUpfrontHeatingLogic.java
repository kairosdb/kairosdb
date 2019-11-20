package org.kairosdb.datastore.cassandra.cache;

import static org.kairosdb.core.tiers.MetricNameUtils.metricNameToCheckId;

public class CacheUpfrontHeatingLogic {

    public boolean isHeatingNeeded(final String metricName, final long currentTime, final long rowTime) {
        final int checkId = metricNameToCheckId(metricName).orElse(0);
        final long gracePeriod = 90; // minutes
        final long currentMinuteOfBucket = (currentTime - rowTime) / 1000 / 60;
        final long currentMinuteOfGracePeriod = currentMinuteOfBucket % gracePeriod;
        return checkId % gracePeriod == currentMinuteOfGracePeriod;
    }
}
