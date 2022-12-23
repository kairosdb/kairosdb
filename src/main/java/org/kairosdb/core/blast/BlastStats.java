package org.kairosdb.core.blast;

import org.kairosdb.metrics4j.annotation.Key;
import org.kairosdb.metrics4j.collectors.LongCollector;

public interface BlastStats
{
	LongCollector submission();
}
