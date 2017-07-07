package org.kairosdb.eventbus;

import org.junit.Test;

public class EventBusWithFiltersTest
{
    @Test(expected = NullPointerException.class)
    public void test_constructor_nullConfig_invalid()
    {
        new EventBusWithFilters(null);
    }
}