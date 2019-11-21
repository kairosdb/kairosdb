package org.kairosdb.datastore.cassandra.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CacheWarmingUpLogic {
    private static final Logger logger = LoggerFactory.getLogger(CacheWarmingUpLogic.class);

    public boolean isWarmingUpNeeded(final int hashCode, final long currentTime, final long nextBucketStartsAt,
                                     final int minutesBeforeNextBucket, int rowSize) {
        final long warmingUpPeriodStartsAt = nextBucketStartsAt - minutesBeforeNextBucket * 1000 * 60;
        if (currentTime < warmingUpPeriodStartsAt) {
            return false;
        }
        final int numberOfRows = minutesBeforeNextBucket / rowSize;
        final long currentRowOfGracePeriod = (currentTime - warmingUpPeriodStartsAt) / 1000 / 60 / rowSize;
        boolean result = hashCode % numberOfRows == currentRowOfGracePeriod;
        logger.debug(String.format("Result '%b' is calculated based on following: " +
                        "hash code of byte buffer = '%d', " +
                        "number of rows = '%d', current row of grace period = '%d', " +
                        "hashCode %% numberOfRows is '%d'",
                result, hashCode, numberOfRows, currentRowOfGracePeriod, hashCode % numberOfRows));
        return result;
    }
}
