package org.kairosdb.core.processingstage.metadata;

import com.google.common.collect.ImmutableList;
import org.kairosdb.core.annotation.FeatureCompoundProperty;
import org.kairosdb.core.annotation.FeatureProperty;
import org.kairosdb.core.annotation.ValidationProperty;

import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang3.StringUtils.isEmpty;

public class FeaturePropertyMetadata
{
    private String name;
    private String label;
    private String description;
    private boolean optional;
    private String type;
    private String[] options;
    private String defaultValue;
    private String autocomplete;
    private boolean multiline;
    private ImmutableList<FeatureValidationMetadata> validations;
    private ImmutableList<FeaturePropertyMetadata> properties;

    public FeaturePropertyMetadata(String name, String type, String options, String defaultValue, FeatureProperty property)
            throws ClassNotFoundException
    {
        this.name = isEmpty(property.name()) ? name : property.name();
        this.label = isEmpty(property.label()) ? name : property.label();
        this.description = property.description();
        this.optional = property.optional();
        this.type = isEmpty(property.type()) ? type : property.type();
        this.options = options == null ? property.options() : options.split(",");
        this.defaultValue = isEmpty(property.default_value()) ? defaultValue : property.default_value();
        this.autocomplete = property.autocomplete();
        this.multiline = property.multiline();
        this.validations = extractValidators(property);

        fixupName();
    }

    public FeaturePropertyMetadata(String name, FeatureCompoundProperty property, List<FeaturePropertyMetadata> properties)
    {
        this.name = isEmpty(property.name()) ? name : property.name();
        this.label = checkNotNull(property.label(), "Label cannot be null");
        this.type = "Object";

        Comparator<FeaturePropertyMetadata> comparator = property.order().length > 0 ?
                new ExplicitComparator(Arrays.asList(property.order())) :
                new LabelComparator();
        properties.sort(comparator);
        this.properties = ImmutableList.copyOf(properties);

        fixupName();
    }

    private void fixupName()
    {
        if (this.name.startsWith("m_"))
            this.name = this.name.substring(2);
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

    public ImmutableList<FeatureValidationMetadata> getValidations() { return validations; }

    public ImmutableList<FeaturePropertyMetadata> getProperties()
    {
        return properties;
    }

    private ImmutableList<FeatureValidationMetadata> extractValidators(FeatureProperty property)
    {
        LinkedList<FeatureValidationMetadata> validations = new LinkedList<FeatureValidationMetadata>();

        for (ValidationProperty validator : property.validations())
            validations.addFirst(new FeatureValidationMetadata(validator.expression(), validator.type(), validator.message()));
        return ImmutableList.copyOf(validations);
    }

    private class LabelComparator implements Comparator<FeaturePropertyMetadata>
    {
        @Override
        public int compare(FeaturePropertyMetadata o1, FeaturePropertyMetadata o2)
        {
            return o1.getLabel().compareTo(o2.getLabel());
        }
    }

    private class ExplicitComparator implements Comparator<FeaturePropertyMetadata>
    {
        private List<String> order;

        private ExplicitComparator(List<String> order)
        {
            this.order = order;
        }

        public int compare(FeaturePropertyMetadata left, FeaturePropertyMetadata right)
        {
            return Integer.compare(order.indexOf(left.getLabel()), order.indexOf(right.getLabel()));
        }
    }
}
