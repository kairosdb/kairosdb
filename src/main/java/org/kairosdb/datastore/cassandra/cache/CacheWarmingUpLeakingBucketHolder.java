package org.kairosdb.datastore.cassandra.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class CacheWarmingUpLeakingBucketHolder {
    public static final Logger logger = LoggerFactory.getLogger(CacheWarmingUpLeakingBucketHolder.class);

    private final AtomicLong leakingBucket = new AtomicLong(1000);

    public AtomicLong getLeakingBucket() {
        return leakingBucket;
    }

    public void refillBucket(int rps) {
        leakingBucket.set(rps);
    }
}
