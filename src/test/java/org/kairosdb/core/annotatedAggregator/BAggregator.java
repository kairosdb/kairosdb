package org.kairosdb.core.annotatedAggregator;


import org.kairosdb.core.aggregator.Aggregator;
import org.kairosdb.core.annotation.QueryProperty;
import org.kairosdb.core.datastore.DataPointGroup;

public class BAggregator implements Aggregator
{
    @QueryProperty(
            name = "inherited",
            label = "Inherited",
            description = "This is alpha",
            type = "int",
            default_value = "1"
    )
    private int alpha;

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
