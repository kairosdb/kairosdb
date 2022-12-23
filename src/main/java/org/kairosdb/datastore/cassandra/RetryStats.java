package org.kairosdb.datastore.cassandra;

import org.kairosdb.metrics4j.annotation.Help;
import org.kairosdb.metrics4j.annotation.Key;
import org.kairosdb.metrics4j.collectors.LongCollector;

import java.util.List;

public interface RetryStats
{
	//kairosdb.datastore.cassandra.retry_count
	@Help("Retry counters for Cassandra client")
	LongCollector retryCount(@Key("cluster")String clusterName, @Key("retry_type")String retryType);
}
