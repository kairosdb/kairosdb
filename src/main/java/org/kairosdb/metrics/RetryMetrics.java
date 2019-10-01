package org.kairosdb.metrics;

import org.kairosdb.metrics4j.annotation.Key;
import org.kairosdb.metrics4j.collectors.LongCollector;

import java.util.List;

public interface RetryMetrics
{
	LongCollector retryCount(@Key("host")String hostName, @Key("cluster")String clusterName, @Key("retry_type")String retryType);
}
