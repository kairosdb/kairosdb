package org.kairosdb.datastore.cassandra;

import java.util.List;

public class CQLQueryRunnerParams {
    private final List<DataPointsRowKey> rowKeys;
    private final int startTime;
    private final int endTime;
    private final boolean descending;

    public CQLQueryRunnerParams(List<DataPointsRowKey> rowKeys, int startTime, int endTime, boolean descending) {
        this.rowKeys = rowKeys;
        this.startTime = startTime;
        this.endTime = endTime;
        this.descending = descending;
    }

    public List<DataPointsRowKey> getRowKeys() {
        return rowKeys;
    }

    public int getStartTime() {
        return startTime;
    }

    public int getEndTime() {
        return endTime;
    }

    public boolean isDescending() {
        return descending;
    }
}