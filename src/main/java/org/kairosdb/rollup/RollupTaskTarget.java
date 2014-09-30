package org.kairosdb.rollup;

import org.kairosdb.core.aggregator.Aggregator;

import java.util.List;
import java.util.Map;

/**
 Target for a roll up task. The target identifies new metric that will be
 created for the roll up, additional tags that will be added to each data point,
 and the aggregations that will be performed.
 */
public class RollupTaskTarget
{
	private final String name;
	private final Map<String, String> tags;
	private final List<Aggregator> aggregators;

	public RollupTaskTarget(String name, Map<String, String> tags, List<Aggregator> aggregators)
	{
		this.name = name;
		this.tags = tags;
		this.aggregators = aggregators;
	}

}
