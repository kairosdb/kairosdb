package org.kairosdb.core.aggregator;

import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.plugin.Aggregator;

public class InvalidAggregator implements Aggregator
{
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
