package org.kairosdb.core.datastore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A map which is shared over multiple data store queries that are part of the same larger composite operation.
 *
 * When closed, an instance will loop over all of its values and call {@link AutoCloseable#close()} on them
 * if they implement the {@link AutoCloseable} interface.
 */
public class DatastoreQueryContext implements AutoCloseable
{

    private static final Logger logger = LoggerFactory.getLogger(DatastoreQueryContext.class);

    private final String identifier;
    private final boolean atomic;
    private final Map<Object, Object> dataMap = new HashMap<>();

    /**
     * Factory method to create an instance for a single atomic (i.e. non-composite) operation.
     */
    public static DatastoreQueryContext createAtomic()
    {
        return new DatastoreQueryContext("atomic operation", true);
    }

    /**
     * Factory method to create an instance with an identifier to identify the encompasing composite operation.
     */
    public static DatastoreQueryContext create(String identifier)
    {
        return new DatastoreQueryContext(checkNotNull(identifier), false);
    }

    private DatastoreQueryContext(String identifier, boolean atomic)
    {
        this.identifier = identifier;
        this.atomic = atomic;
    }

    public boolean isAtomic() {
        return atomic;
    }

    public void setData(Object key, Object value)
    {
        dataMap.put(key, value);
    }

    public Object getData(Object key)
    {
        return dataMap.get(key);
    }

    @Override
    public void close() {
        for (Map.Entry<Object, Object> entry : dataMap.entrySet()) {
            if (entry.getValue() instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) entry.getValue()).close();
                } catch (Exception e) {
                    logger.error("Error closing value {} under key {}", entry.getValue(), entry.getKey(), e);
                }
            } else {
                logger.debug("Skipping non-closeable object {} under key {}", entry.getValue(), entry.getKey());
            }
        }
    }

    @Override
    public String toString() {
        return "DatastoreQueryContext{" +
                "identifier='" + identifier + '\'' +
                ", dataMap=" + dataMap +
                '}';
    }
}
