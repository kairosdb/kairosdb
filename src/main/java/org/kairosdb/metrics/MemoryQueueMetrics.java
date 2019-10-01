package org.kairosdb.metrics;

import org.kairosdb.metrics4j.annotation.Key;
import org.kairosdb.metrics4j.collectors.LongCollector;

public interface MemoryQueueMetrics
{
	LongCollector processCount(@Key("host")String host);
}
