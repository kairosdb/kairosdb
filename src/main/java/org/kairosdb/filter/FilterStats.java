package org.kairosdb.filter;

import org.kairosdb.metrics4j.collectors.LongCollector;

public interface FilterStats
{
	LongCollector skippedMetrics();
}
