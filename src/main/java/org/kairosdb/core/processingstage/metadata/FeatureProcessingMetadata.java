package org.kairosdb.core.processingstage.metadata;

import com.google.common.collect.ImmutableList;

import java.util.Comparator;
import java.util.List;

public class FeatureProcessingMetadata
{
    private final String name;
    private final String label;
    private final ImmutableList<FeatureProcessorMetadata> properties;

    public FeatureProcessingMetadata(String name, String label, List<FeatureProcessorMetadata> properties)
    {
        this.name = name;
        this.label = label;

        properties.sort(Comparator.comparing(FeatureProcessorMetadata::getLabel));
        this.properties = ImmutableList.copyOf(properties);
    }

    public String getName() { return name; }

    public String getLabel() { return label; }

    public ImmutableList<FeatureProcessorMetadata> getProperties() { return properties; }
}
