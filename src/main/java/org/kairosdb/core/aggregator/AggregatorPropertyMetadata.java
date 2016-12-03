package org.kairosdb.core.aggregator;

import com.google.inject.Inject;
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

    @Inject
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
}
