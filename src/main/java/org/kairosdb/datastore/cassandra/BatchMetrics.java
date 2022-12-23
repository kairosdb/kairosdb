package org.kairosdb.datastore.cassandra;

import org.kairosdb.metrics4j.annotation.Key;
import org.kairosdb.metrics4j.collectors.LongCollector;

public interface BatchMetrics
{
	LongCollector writeBatchSize(@Key("table")String table);
}
