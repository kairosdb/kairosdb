package org.kairosdb.core.tiers;

public class QueryRejectedException extends Exception {
    private final String metricName;

    public QueryRejectedException(final String metricName) {
        super("Query to metric " + metricName + " was rejected due to tier limitations");
        this.metricName = metricName;
    }
}
