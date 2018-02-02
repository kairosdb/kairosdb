package org.kairosdb.rollup;

public class RollupQueryMetricStatus
{
    private String metricName;
    private String lastExecuted;
    private long dataPointCount;
    private long executionLength;
    private String errorMessage;

    public RollupQueryMetricStatus(String metricName, String lastExecuted, long dataPointCount, long executionLength)
    {
        this.metricName = metricName;
        this.lastExecuted = lastExecuted;
        this.dataPointCount = dataPointCount;
        this.executionLength = executionLength;
    }

    public RollupQueryMetricStatus(String metricName, String lastExecuted, long executionLength, String errorMessage)
    {
        this.metricName = metricName;
        this.lastExecuted = lastExecuted;
        this.executionLength = executionLength;
        this.errorMessage = errorMessage;
    }

    public String getMetricName()
    {
        return metricName;
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
