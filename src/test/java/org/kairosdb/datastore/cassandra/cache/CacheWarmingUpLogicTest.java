package org.kairosdb.datastore.cassandra.cache;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CacheWarmingUpLogicTest {
    private CacheWarmingUpLogic logic;
    private final int MINUTES = 1000 * 60;

    @Before
    public void setUp() {
        logic = new CacheWarmingUpLogic();
    }

    @Test
    public void testNotFailForUnknownMetric() {
        final boolean result = logic.isWarmingUpNeeded("bla-bla-bla", 100 * MINUTES, 5 * MINUTES, 120 * MINUTES, 1);
        Assert.assertFalse(result);
    }

    @Test
    public void testShouldNotWarmUpToEarly() {
        final boolean result = logic.isWarmingUpNeeded("bla-bla-bla", 104 * MINUTES, 5 * MINUTES, 120 * MINUTES, 20);
        Assert.assertFalse(result);
    }

    @Test
    public void testShouldWarmUpAllNonZmonMetricOnTheFirstMinute() {
        final boolean result = logic.isWarmingUpNeeded("bla-bla-bla", 105 * MINUTES, 5 * MINUTES, 120 * MINUTES, 20);
        Assert.assertTrue(result);
    }

    @Test
    public void testWarmingUpNeededForCheckIdEqualCurrentMinute() {
        final boolean result = logic.isWarmingUpNeeded("zmon.check.5", 101 * MINUTES, 0, 120 * MINUTES, 30);
        Assert.assertTrue(result);
    }

    @Test
    public void testWarmingUpNeededForCheckIdModuleCurrentMinute() {
        final boolean result = logic.isWarmingUpNeeded("zmon.check.65", 110 * MINUTES, 10 * MINUTES, 120 * MINUTES, 30);
        Assert.assertTrue(result);
    }

    @Test
    public void testWarmingUpPercentageIsCorrect() {
        long now = System.currentTimeMillis();
        long bucketSize = 120 * MINUTES;
        long rowTime = now - bucketSize / 2;
        int cnt = 0;
        for (int i = 1; i <= 900; i++) {
            boolean needed = logic.isWarmingUpNeeded("zmon.check." + i, now, rowTime, bucketSize, 90);
            if (needed) {
                cnt++;
            }
        }
        Assert.assertEquals(20, cnt);
    }

}