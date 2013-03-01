package net.opentsdb.core.aggregator;

import java.util.HashMap;
import java.util.Map;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 3/1/13
 Time: 1:14 PM
 To change this template use File | Settings | File Templates.
 */
public class GuiceAggregatorFactory implements AggregatorFactory
{
	private Map<String, Aggregator> m_aggregators = new HashMap<String, Aggregator>();

	public GuiceAggregatorFactory()
	{
		m_aggregators.put("sum", new SumAggregator());
		m_aggregators.put("min", new MinAggregator());
		m_aggregators.put("max", new MaxAggregator());
		m_aggregators.put("avg", new AvgAggregator());
		m_aggregators.put("dev", new StdAggregator());
		m_aggregators.put("sort", new SortAggregator());
	}

	public Aggregator createAggregator(String name)
	{
		return (m_aggregators.get(name));
	}
}
