package org.kairosdb.rollup;

import org.kairosdb.metrics4j.annotation.Key;
import org.kairosdb.metrics4j.collectors.DurationCollector;
import org.kairosdb.metrics4j.collectors.LongCollector;

import java.util.Map;

public interface RollupStats
{
	DurationCollector executionTime(@Key("rollup")String rollup, @Key("rollup-task")String task, @Key("status")String status);

	LongCollector jobRunCount(@Key("rollup")String rollup, @Key("rollup-task") String name);

	LongCollector jobQueryCount(@Key("rollup")String rollup, @Key("rollup-task") String name);
}
