package org.kairosdb.core.aggregator;

import org.kairosdb.core.datastore.DataPointGroup;

/**
 Created by bhawkins on 5/18/17.
 */
public class DropAggregator implements Aggregator
{
	public enum Drop
	{
		HIGH, LOW, BOTH
	};



	@Override
	public DataPointGroup aggregate(DataPointGroup dataPointGroup)
	{
		return null;
	}

	@Override
	public boolean canAggregate(String groupType)
	{
		return false;
	}

	@Override
	public String getAggregatedGroupType(String groupType)
	{
		return null;
	}




}
