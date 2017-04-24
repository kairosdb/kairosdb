package org.kairosdb.core.processingstage.metadata;

import com.google.common.collect.ImmutableList;

import java.util.Comparator;
import java.util.List;

public class QueryProcessorMetadata
{
    private final String name;
    private final String label;
    private final String description;
    private final ImmutableList<QueryPropertyMetadata> properties;

    public QueryProcessorMetadata(String name, String label, String description, List<QueryPropertyMetadata> properties)
    {
        this.name = name;
        this.label = label;
        this.description = description;

        properties.sort(Comparator.comparing(QueryPropertyMetadata::getLabel));
        this.properties = ImmutableList.copyOf(properties);
    }

    public String getName()
    {
        return name;
    }

    public String getLabel() { return label; }

    public String getDescription()
    {
        return description;
    }

    public ImmutableList<QueryPropertyMetadata> getProperties()
    {
        return properties;
    }
}
