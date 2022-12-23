package org.kairosdb.core.telnet;

import org.kairosdb.metrics4j.annotation.Key;
import org.kairosdb.metrics4j.collectors.LongCollector;

public interface TelnetStats
{
	LongCollector request(@Key("method") String method);
}
