package org.kairosdb.datastore.cassandra.cache;

import static org.kairosdb.core.tiers.MetricNameUtils.metricNameToCheckId;

public class CacheUpfrontHeatingLogic {

    public boolean isHeatingNeeded(final String metricName, final long currentTime, final long currentBucketStart,
                                   final long bucketSize, final int minutesBeforeNextBucket) {
        final int checkId = metricNameToCheckId(metricName).orElse(0);
        final long nextBucketStartsAt = currentBucketStart + bucketSize;
        final long heatingPeriodStartsAt = nextBucketStartsAt - minutesBeforeNextBucket * 1000 * 60;
        if (currentTime < heatingPeriodStartsAt) {
            return false;
        }
        final long currentMinuteOfBucket = (currentTime - heatingPeriodStartsAt) / 1000 / 60;
        final long currentMinuteOfGracePeriod = currentMinuteOfBucket % minutesBeforeNextBucket;
        return checkId % minutesBeforeNextBucket == currentMinuteOfGracePeriod;
    }
}
