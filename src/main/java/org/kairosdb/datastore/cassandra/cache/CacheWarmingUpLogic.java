package org.kairosdb.datastore.cassandra.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.kairosdb.core.tiers.MetricNameUtils.metricNameToCheckId;

public class CacheWarmingUpLogic {
    private static final Logger logger = LoggerFactory.getLogger(CacheWarmingUpLogic.class);
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
        final long currentRowOfGracePeriod = (currentTime - warmingUpPeriodStartsAt) / 1000 / 60 / ROW_SIZE;
        boolean result = checkId % numberOfRows == currentRowOfGracePeriod;
        logger.error(String.format("Result '%b' is calculated based on following: " +
                        "metric name = '%s', check id = '%d', " +
                        "number of rows = '%d', current row of grace period = '%d', " +
                        "checkId %% numberOfRows is '%d'",
                result, metricName, checkId, numberOfRows, currentRowOfGracePeriod, checkId % numberOfRows));
        return result;
    }
}
