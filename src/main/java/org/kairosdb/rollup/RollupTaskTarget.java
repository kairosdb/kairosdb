package org.kairosdb.rollup;

import org.kairosdb.core.aggregator.Aggregator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;

// todo Delete this class
/**
 Target for a roll up task. The target identifies a new metric that will be
 created for the roll up, additional tags that will be added to each data point,
 and the aggregations that will be performed.
 */
public class RollupTaskTarget
{
	private final String name;
	private final Map<String, String> tags = new HashMap<String, String>();
	private final transient List<Aggregator> aggregators = new ArrayList<Aggregator>();

	public RollupTaskTarget(String name)
	{
		checkNotNullOrEmpty(name);
		this.name = name;
	}

	public RollupTaskTarget addTag(String name, String value)
	{
		checkNotNullOrEmpty(name);
		checkNotNullOrEmpty(value);

		tags.put(name, value);
		return this;
	}

	public RollupTaskTarget addAggregator(Aggregator aggregator)
	{
		checkNotNull(aggregator);

		aggregators.add(aggregator);
		return this;
	}

	public String getName()
	{
		return name;
	}

	public Map<String, String> getTags()
	{
		return tags;
	}

	public List<Aggregator> getAggregators()
	{
		return aggregators;
	}
}
