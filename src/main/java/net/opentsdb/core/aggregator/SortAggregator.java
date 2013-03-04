package net.opentsdb.core.aggregator;

import net.opentsdb.core.aggregator.annotation.AggregatorName;
import net.opentsdb.core.datastore.DataPointGroup;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 3/1/13
 Time: 2:10 PM
 To change this template use File | Settings | File Templates.
 */
@AggregatorName(name="sort")
public class SortAggregator extends SortedAggregator
{
	@Override
	protected DataPointGroup aggregate(DataPointGroup dataPointGroup)
	{
		return (dataPointGroup);
	}
}
