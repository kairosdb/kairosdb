package org.kairosdb.eventbus;

import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Event bus configuration. Priority on a filter is set by adding the prefix kairosdb.eventbus.filter.priority.
 * to your class name. A filter priority is between 0 and 100 inclusive where 0 is the highest priority. The
 * default priority (if unspecified) is 50.
 * For example,
 *
 * kairosdb.eventbus.filter.priority.org.myStuff.filters.Filter1=30
 */
public class EventBusConfiguration
{
    private static final Logger logger = LoggerFactory.getLogger(EventBusConfiguration.class);
    private static final String KAIROSDB_EVENTBUS_FILTER_PRIORITY_PREFIX = "kairosdb.eventbus.filter.priority.";
    private static final int PREFIX_LENGTH = KAIROSDB_EVENTBUS_FILTER_PRIORITY_PREFIX.length();

    private final Map<String, Integer> priorities = new HashMap<>();

    @Inject
    public EventBusConfiguration(Properties properties)
    {
        checkNotNull(properties, "properties cannot be null");

        for (Object o : properties.keySet()) {
            String property = (String) o;
            if (property.startsWith(KAIROSDB_EVENTBUS_FILTER_PRIORITY_PREFIX))
            {
                String className = property.substring(property.indexOf(KAIROSDB_EVENTBUS_FILTER_PRIORITY_PREFIX) + PREFIX_LENGTH);
                try {
                    int priority = Integer.parseInt(properties.getProperty(property));
                    priorities.put(className, priority);
                }
                catch (NumberFormatException e) {
                    logger.error("Priority is invalid " + properties.getProperty(property));
                }
            }
        }
    }

    public int getFilterPriority(String filterClassName)
    {
        return priorities.getOrDefault(filterClassName, PipelineRegistry.DEFAULT_PRIORITY);
    }
}
