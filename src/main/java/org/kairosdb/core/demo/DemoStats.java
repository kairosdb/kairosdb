package org.kairosdb.core.demo;

import org.kairosdb.metrics4j.collectors.LongCollector;

public interface DemoStats
{
	LongCollector submission();
}
