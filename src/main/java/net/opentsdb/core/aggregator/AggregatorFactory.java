package net.opentsdb.core.aggregator;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 3/1/13
 Time: 1:20 PM
 To change this template use File | Settings | File Templates.
 */
public interface AggregatorFactory
{
	public Aggregator createAggregator(String name);
}
