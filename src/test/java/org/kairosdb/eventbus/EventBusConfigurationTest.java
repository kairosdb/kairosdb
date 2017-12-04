package org.kairosdb.eventbus;

import org.junit.Test;

import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class EventBusConfigurationTest
{

    @Test(expected = NullPointerException.class)
    public void test_constructor_nullProperties_invalid()
    {
        new EventBusConfiguration(null);
    }

    @Test
    public void test()
    {
        Properties properties = new Properties();
        properties.put("kairosdb.eventbus.filter.priority.com.foo.Filter1", "10");
        properties.put("kairosdb.eventbus.filter.priority.com.bar.Filter2", "20");
        properties.put("kairosdb.eventbus.filter.priority.com.fi.Filter3", "30");
        properties.put("kairosdb.eventbus.filter.priority.org.apache.Filter4", "40");

        EventBusConfiguration config = new EventBusConfiguration(properties);

        assertThat(config.getFilterPriority("com.foo.Filter1"), equalTo(10));
        assertThat(config.getFilterPriority("com.bar.Filter2"), equalTo(20));
        assertThat(config.getFilterPriority("com.fi.Filter3"), equalTo(30));
        assertThat(config.getFilterPriority("org.apache.Filter4"), equalTo(40));
        assertThat(config.getFilterPriority("com.foo.Bogus"), equalTo(PipelineRegistry.DEFAULT_PRIORITY));
    }

    @Test
    public void test_invalid_priority()
    {
        Properties properties = new Properties();
        properties.put("kairosdb.eventbus.filter.priority.com.foo.Filter1", "10.5");

        EventBusConfiguration config = new EventBusConfiguration(properties);

        assertThat(config.getFilterPriority("com.foo.Filter1"), equalTo(PipelineRegistry.DEFAULT_PRIORITY));
    }

}