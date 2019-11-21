package org.kairosdb.datastore.cassandra.cache;

import static org.kairosdb.core.tiers.MetricNameUtils.metricNameToCheckId;

public class CacheWarmingUpLogic {
    private static final int ROW_SIZE = 2; // minutes

    public boolean isWarmingUpNeeded(final String metricName, final long currentTime, final long currentBucketStart,
                                     final long bucketSize, final int minutesBeforeNextBucket) {
        final int checkId = metricNameToCheckId(metricName).orElse(0);
        final long nextBucketStartsAt = currentBucketStart + bucketSize;
        final long warmingUpPeriodStartsAt = nextBucketStartsAt - minutesBeforeNextBucket * 1000 * 60;
        if (currentTime < warmingUpPeriodStartsAt) {
            return false;
        }
        final int numberOfRows = minutesBeforeNextBucket / ROW_SIZE;
        final long currentRowOfBucket = (currentTime - warmingUpPeriodStartsAt) / 1000 / 60 / ROW_SIZE;
        final long currentRowOfGracePeriod = currentRowOfBucket % numberOfRows;
        return checkId % numberOfRows == currentRowOfGracePeriod;
    }
}
