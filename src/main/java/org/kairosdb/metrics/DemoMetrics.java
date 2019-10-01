package org.kairosdb.metrics;

import org.kairosdb.metrics4j.annotation.Key;
import org.kairosdb.metrics4j.collectors.LongCollector;

public interface DemoMetrics
{
	LongCollector submissionCount(@Key("host")String host);
}
