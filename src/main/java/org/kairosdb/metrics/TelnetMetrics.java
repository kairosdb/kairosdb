package org.kairosdb.metrics;

import org.kairosdb.metrics4j.annotation.Key;
import org.kairosdb.metrics4j.collectors.LongCollector;

public interface TelnetMetrics
{
	LongCollector telnetRequestCount(@Key("host")String host, @Key("method")String method);
}
