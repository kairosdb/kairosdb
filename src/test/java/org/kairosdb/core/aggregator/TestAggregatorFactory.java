package org.kairosdb.core.aggregator;

import org.kairosdb.core.aggregator.annotation.AggregatorName;

import java.util.HashMap;
import java.util.Map;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 3/1/13
 Time: 1:32 PM
 To change this template use File | Settings | File Templates.
 */
public class TestAggregatorFactory implements AggregatorFactory
{
	private Map<String, Aggregator> m_aggregators = new HashMap<String, Aggregator>();

	public TestAggregatorFactory()
	{
		addAggregator(new SumAggregator());
		addAggregator(new MinAggregator());
		addAggregator(new MaxAggregator());
		addAggregator(new AvgAggregator());
		addAggregator(new StdAggregator());
		addAggregator(new SortAggregator());
	}

	private void addAggregator(Aggregator agg)
	{
		String name = (agg.getClass().getAnnotation(AggregatorName.class)).name();
		m_aggregators.put(name, agg);
	}

	public Aggregator createAggregator(String name)
	{
		return (m_aggregators.get(name));
	}
}
