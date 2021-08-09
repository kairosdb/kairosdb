package org.kairosdb.rollup;

public class RollupQueryMetricStatus
{
    private String metricName;
    private String lastExecuted;
    private long dataPointCount;
    private long executionLength;
    private String errorMessage;
    private long lastExecutionTime; // added to be backward compatible

    public RollupQueryMetricStatus(String metricName, String lastExecuted, long dataPointCount, long executionLength, long lastExecutionTime)
    {
        this.metricName = metricName;
        this.lastExecuted = lastExecuted;
        this.dataPointCount = dataPointCount;
        this.executionLength = executionLength;
        this.lastExecutionTime = lastExecutionTime;
    }

    public RollupQueryMetricStatus(String metricName, String lastExecuted, long executionLength, long lastExecutionTime, String errorMessage)
    {
        this.metricName = metricName;
        this.lastExecuted = lastExecuted;
        this.executionLength = executionLength;
        this.errorMessage = errorMessage;
        this.lastExecutionTime = lastExecutionTime;
    }

    public String getMetricName()
    {
        return metricName;
    }

    public long getLastExecutionTime()
    {
        return lastExecutionTime;
    }

    public String getLastExecuted()
    {
        return lastExecuted;
    }

    public long getDataPointCount()
    {
        return dataPointCount;
    }

    public long getExecutionLength()
    {
        return executionLength;
    }

    public String getErrorMessage()
    {
        return errorMessage;
    }

    public boolean hasError()
    {
        return !errorMessage.isEmpty();
    }
}
