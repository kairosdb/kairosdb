package org.kairosdb.rollup;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;

public class RollupTaskStatus
{
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("dd MMM yyyy KK:mm:ss a");
    private static final String NEVER_SCHEDULED = "Never";

    private final List<RollupQueryMetricStatus> statuses = new ArrayList<>();
    private final String executingHost;
    private String nextScheduled;

    public RollupTaskStatus(Date nextExecutionTime, String executingHost)
    {
        setNextScheduled(nextExecutionTime);
        this.executingHost = checkNotNullOrEmpty(executingHost);
    }

    public static RollupQueryMetricStatus createQueryMetricStatus(String metricName, long lastExecuted, long dataPointCount, long executionLength)
    {
        return new RollupQueryMetricStatus(metricName, getLastExecutedTime(lastExecuted), dataPointCount, executionLength);
    }

    public static RollupQueryMetricStatus createErrorQueryMetricStatus(String metricName, long lastExecuted, String errorMessage, long executionLength)
    {
        return new RollupQueryMetricStatus(metricName, getLastExecutedTime(lastExecuted), executionLength, errorMessage);
    }

    public String getNextScheduled()
    {
        return nextScheduled;
    }

    public void setNextScheduled(Date nextScheduled)
    {
        if (nextScheduled != null) {
            this.nextScheduled = DATE_FORMAT.format(nextScheduled);
        }
        else
        {
            this.nextScheduled = NEVER_SCHEDULED;
        }
    }

    public void addStatus(RollupQueryMetricStatus status)
    {
        statuses.add(status);
    }

    public List<RollupQueryMetricStatus> getStatuses()
    {
        return statuses;
    }

    private static String getLastExecutedTime(long lastExecuted)
    {
        if (lastExecuted == 0)
        {
            return NEVER_SCHEDULED;
        }
        return DATE_FORMAT.format(lastExecuted);
    }

    public String getExecutingHost()
    {
        return executingHost;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RollupTaskStatus that = (RollupTaskStatus) o;

        if (!statuses.equals(that.statuses)) {
            return false;
        }
        if (!executingHost.equals(that.executingHost)) {
            return false;
        }
        return nextScheduled.equals(that.nextScheduled);
    }

    @Override
    public int hashCode()
    {
        int result = statuses.hashCode();
        result = 31 * result + executingHost.hashCode();
        result = 31 * result + nextScheduled.hashCode();
        return result;
    }
}
