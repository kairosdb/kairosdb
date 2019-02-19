package org.kairosdb.core.demo;

import org.kairosdb.metrics4j.annotation.Key;
import org.kairosdb.metrics4j.stats.Counter;

public interface DemoMetrics
{
	Counter submissionCount(@Key("host")String host);
}
