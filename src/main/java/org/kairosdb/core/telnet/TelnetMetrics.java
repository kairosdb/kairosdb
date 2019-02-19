package org.kairosdb.core.telnet;

import org.kairosdb.metrics4j.annotation.Key;
import org.kairosdb.metrics4j.stats.Counter;

public interface TelnetMetrics
{
	Counter telnetRequestCount(@Key("host")String host, @Key("method")String method);
}
