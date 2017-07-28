package org.kairosdb.core.processingstage.metadata;

import com.google.common.collect.ImmutableList;

import java.util.Comparator;
import java.util.List;

public class QueryProcessingStageMetadata
{
    private final String name;
    private final String label;
    private final ImmutableList<QueryProcessorMetadata> properties;

    public QueryProcessingStageMetadata(String name, String label, List<QueryProcessorMetadata> properties)
    {
        this.name = name;
        this.label = label;

        properties.sort(Comparator.comparing(QueryProcessorMetadata::getLabel));
        this.properties = ImmutableList.copyOf(properties);
    }

    public String getName() { return name; }

    public String getLabel() { return label; }

    public ImmutableList<QueryProcessorMetadata> getProperties() { return properties; }
}
