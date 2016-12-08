package org.kairosdb.core.aggregator.json;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import org.kairosdb.core.aggregator.annotation.AggregatorCompoundProperty;
import org.kairosdb.core.aggregator.annotation.AggregatorProperty;

public class AggregatorPropertyMetadata
{
    private String name;
    private String label;
    private String description;
    private boolean optional;
    private String type;
    private String[] options;
    private Object defaultValue;
    private String validation;
    private ImmutableList<AggregatorPropertyMetadata> properties;

    public AggregatorPropertyMetadata(AggregatorProperty property)
    {
        this.name = property.name();
        this.label = property.label();
        this.description = property.description();
        this.optional = property.optional();
        this.type = property.type();
        this.options = property.options();
        this.defaultValue = property.default_value();
        this.validation = property.validation();
    }

    public AggregatorPropertyMetadata(AggregatorCompoundProperty compoundProperty)
    {
        this.label = compoundProperty.label();

        Builder<AggregatorPropertyMetadata> builder = ImmutableList.builder();
        for (AggregatorProperty aggregatorProperty : compoundProperty.properties()) {
            builder.add(new AggregatorPropertyMetadata(aggregatorProperty));
        }
        properties = builder.build();
    }

    public String getName()
    {
        return name;
    }

    public String getLabel()
    {
        return label;
    }

    public String getDescription()
    {
        return description;
    }

    public boolean isOptional()
    {
        return optional;
    }

    public String getType()
    {
        return type;
    }

    public String[] getOptions()
    {
        return options;
    }

    public Object getDefaultValue()
    {
        return defaultValue;
    }

    public String getValidation()
    {
        return validation;
    }

    public ImmutableList<AggregatorPropertyMetadata> getProperties()
    {
        return properties;
    }
}
