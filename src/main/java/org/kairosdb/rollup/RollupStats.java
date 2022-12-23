package org.kairosdb.rollup;

import org.kairosdb.metrics4j.annotation.Key;
import org.kairosdb.metrics4j.collectors.DurationCollector;

public interface RollupStats
{
	DurationCollector executionTime(@Key("rollup")String rollup, @Key("rollup-task")String task, @Key("status")String status);
}
