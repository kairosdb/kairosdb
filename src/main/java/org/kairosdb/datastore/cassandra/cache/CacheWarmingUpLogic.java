package org.kairosdb.datastore.cassandra.cache;

import static org.kairosdb.core.tiers.MetricNameUtils.metricNameToCheckId;

public class CacheWarmingUpLogic {

    public boolean isWarmingUpNeeded(final String metricName, final long currentTime, final long currentBucketStart,
                                     final long bucketSize, final int minutesBeforeNextBucket) {
        final int checkId = metricNameToCheckId(metricName).orElse(0);
        final long nextBucketStartsAt = currentBucketStart + bucketSize;
        final long warmingUpPeriodStartsAt = nextBucketStartsAt - minutesBeforeNextBucket * 1000 * 60;
        if (currentTime < warmingUpPeriodStartsAt) {
            return false;
        }
        final long currentMinuteOfBucket = (currentTime - warmingUpPeriodStartsAt) / 1000 / 60;
        final long currentMinuteOfGracePeriod = currentMinuteOfBucket % minutesBeforeNextBucket;
        return checkId % minutesBeforeNextBucket == currentMinuteOfGracePeriod;
    }
}
