package org.kairosdb.core.blast;

import org.kairosdb.metrics4j.annotation.Key;
import org.kairosdb.metrics4j.stats.Counter;

public interface BlastMetrics
{
	Counter submissionCount(@Key("host")String host);
}
