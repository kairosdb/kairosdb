package org.kairosdb.datastore.cassandra.cache;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CacheUpfrontHeatingLogicTest {
    private CacheUpfrontHeatingLogic logic;
    private final int MINUTES = 1000 * 60;

    @Before
    public void setUp() {
        logic = new CacheUpfrontHeatingLogic();
    }

    @Test
    public void testNotFailForUnknownMetric() {
        final boolean result = logic.isHeatingNeeded("bla-bla-bla", 100 * MINUTES, 5 * MINUTES);
        Assert.assertFalse(result);
    }

    @Test
    public void testHeatingNeededForCheckIdEqualCurrentMinute() {
        final boolean result = logic.isHeatingNeeded("zmon.check.50", 50 * MINUTES, 0);
        Assert.assertTrue(result);
    }

    @Test
    public void testHeatingNeededForCheckIdModuleCurrentMinute() {
        final boolean result = logic.isHeatingNeeded("zmon.check.10", 110 * MINUTES, 10 * MINUTES);
        Assert.assertTrue(result);
    }

    @Test
    public void testHeatingPercentageIsCorrect() {
        long now = System.currentTimeMillis();
        int cnt = 0;
        for (int i = 1; i <= 900; i++) {
            boolean needed = logic.isHeatingNeeded("zmon.check." + i, now, 0);
            if (needed) {
                cnt++;
            }
        }
        Assert.assertEquals(10, cnt);
    }

}