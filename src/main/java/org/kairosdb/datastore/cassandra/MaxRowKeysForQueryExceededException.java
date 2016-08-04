package org.kairosdb.datastore.cassandra;

public class MaxRowKeysForQueryExceededException extends RuntimeException {
    public MaxRowKeysForQueryExceededException(String message) {
        super(message);
    }
}
