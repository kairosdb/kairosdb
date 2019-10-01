package org.kairosdb.metrics;

import org.kairosdb.metrics4j.annotation.Key;
import org.kairosdb.metrics4j.collectors.LongCollector;

public interface BlastMetrics
{
	LongCollector submissionCount(@Key("host")String host);
}
