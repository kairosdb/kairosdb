package org.kairosdb.core.aggregator;

import org.kairosdb.core.aggregator.annotation.AggregatorName;
import org.kairosdb.core.datastore.DataPointGroup;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 3/1/13
 Time: 2:10 PM
 To change this template use File | Settings | File Templates.
 */
@AggregatorName(name="sort", description = "Sorts the data points according to time from lowest to highest.")
public class SortAggregator extends SortedAggregator
{
	@Override
	protected DataPointGroup aggregate(DataPointGroup dataPointGroup)
	{
		return (dataPointGroup);
	}
}
