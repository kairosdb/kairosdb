package org.kairosdb.core.aggregator;

import com.google.inject.Inject;

public class AggregatorPropertyMetadata
{
    private String name;
    private String type;
    private String[] values;

    @Inject
    public AggregatorPropertyMetadata(String name, String type, String[] values)
    {
        this.name = name;
        this.type = type;
        this.values = values;
    }

    public String getName()
    {
        return name;
    }

    public String getType()
    {
        return type;
    }

    public String[] getValues()
    {
        return values;
    }


}
