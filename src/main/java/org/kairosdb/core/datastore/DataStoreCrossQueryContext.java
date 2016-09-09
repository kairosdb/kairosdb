package org.kairosdb.core.datastore;

import com.datastax.driver.core.ResultSet;

import java.util.EnumMap;
import java.util.HashMap;

/**
 * Created by jmussler on 09.09.16.
 */
public class DataStoreCrossQueryContext extends EnumMap<CrossQueryContextFields, Object> {
    public DataStoreCrossQueryContext() {
        super(CrossQueryContextFields.class);

        put(CrossQueryContextFields.CASSANDRA_ROW_KEYS, new HashMap<String, ResultSet>());
    }
}
