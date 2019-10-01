package org.kairosdb.metrics;

import org.kairosdb.metrics4j.collectors.LongCollector;

public interface BatchMetrics
{
	LongCollector nameBatch();
	LongCollector rowKeyBatch();
	LongCollector datapointsBatch();
}
