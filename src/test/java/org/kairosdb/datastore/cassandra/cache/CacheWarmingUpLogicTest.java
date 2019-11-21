package org.kairosdb.datastore.cassandra.cache;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

public class CacheWarmingUpLogicTest {
    private CacheWarmingUpLogic logic;
    private final int MINUTES = 1000 * 60;

    @Before
    public void setUp() {
        logic = new CacheWarmingUpLogic();
    }

    @Test
    public void testNotFailForUnknownMetric() {
        final boolean result = logic.isWarmingUpNeeded("bla-bla-bla", 100 * MINUTES, 5 * MINUTES, 120 * MINUTES, 1, 2);
        Assert.assertFalse(result);
    }

    @Test
    public void testShouldNotWarmUpToEarly() {
        final long now = System.currentTimeMillis();
        final long bucketStart = now - 45 * 60 * MINUTES;
        final long bucketSize = 48 * 60 * MINUTES;
        final int minutesInterval = 150;
        final boolean result = logic.isWarmingUpNeeded("bla-bla-bla", now, bucketStart, bucketSize, minutesInterval, 2);
        Assert.assertFalse(result);
    }

    @Test
    public void testShouldWarmUpAllNonZmonMetricOnTheFirstMinute() {
        final boolean result = logic.isWarmingUpNeeded("bla-bla-bla", 105 * MINUTES, 5 * MINUTES, 120 * MINUTES, 20, 2);
        Assert.assertTrue(result);
    }

    @Test
    public void testWarmingUpNeededForCheckIdEqualCurrentMinute() {
        final boolean result = logic.isWarmingUpNeeded("zmon.check.5", 101 * MINUTES, 0, 120 * MINUTES, 30, 2);
        Assert.assertTrue(result);
    }

    @Test
    public void testWarmingUpNeededForCheckIdModuleCurrentMinute() {
        final boolean result = logic.isWarmingUpNeeded("zmon.check.65", 110 * MINUTES, 10 * MINUTES, 120 * MINUTES, 30, 2);
        Assert.assertTrue(result);
    }

    @Test
    public void testWarmingUpPercentageDependsOnTheRowSize() {
        for (Integer rowSize : Arrays.asList(1, 2, 10)) {
            long now = System.currentTimeMillis();
            long bucketSize = 120 * MINUTES;
            long rowTime = now - bucketSize / 2;
            int cnt = 0;
            for (int i = 1; i <= 900; i++) {
                boolean needed = logic.isWarmingUpNeeded("zmon.check." + i, now, rowTime, bucketSize, 90, rowSize);
                if (needed) {
                    cnt++;
                }
            }
            Assert.assertEquals(10 * rowSize, cnt);
        }
    }
}