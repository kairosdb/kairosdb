package org.kairosdb.core.aggregator;


import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

public class AggregatorMetadata
{
    private final String name;
    private final String description;
    private final ImmutableList<AggregatorPropertyMetadata> properties;

    @Inject
    public AggregatorMetadata(String name, String description, ImmutableList<AggregatorPropertyMetadata> properties)
    {
        this.name = name;
        this.description = description;
        this.properties = properties;
    }

    public String getName()
    {
        return name;
    }

    public String getDescription()
    {
        return description;
    }

    public ImmutableList<AggregatorPropertyMetadata> getProperties()
    {
        return properties;
    }
}
