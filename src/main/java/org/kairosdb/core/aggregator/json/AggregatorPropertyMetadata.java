package org.kairosdb.core.aggregator.json;

import com.google.common.base.Defaults;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.ClassUtils;
import org.kairosdb.core.aggregator.annotation.AggregatorCompoundProperty;
import org.kairosdb.core.aggregator.annotation.AggregatorProperty;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang.StringUtils.isEmpty;

public class AggregatorPropertyMetadata
{
    private String name;
    private String label;
    private String description;
    private boolean optional;
    private String type;
    private String[] options;
    private String defaultValue;
    private String validation;
    private ImmutableList<AggregatorPropertyMetadata> properties;

    public AggregatorPropertyMetadata(String name, String type, String options, AggregatorProperty property)
            throws ClassNotFoundException
    {
        this.name = isEmpty(property.name()) ? name : property.name();
        this.label = isEmpty(property.label()) ? name : property.label();
        this.description = property.description();
        this.optional = property.optional();
        this.type = isEmpty(property.type()) ? type : property.type();
        this.options = options == null ? property.options() : options.split(",");
        this.defaultValue = isEmpty(property.default_value()) ? calculateDefaultValue() : property.default_value();
        this.validation = property.validation();

        fixupName();
    }

    public AggregatorPropertyMetadata(String name, AggregatorCompoundProperty property, List<AggregatorPropertyMetadata> properties)
    {
        this.name = isEmpty(property.name()) ? name : property.name();
        this.label = checkNotNull(property.label(), "Label cannot be null");
        this.type = "Object";

        Comparator<AggregatorPropertyMetadata> comparator = property.order().length > 0 ?
                new ExplicitComparator(Arrays.asList(property.order())) :
                new LabelComparator();
        properties.sort(comparator);
        this.properties = ImmutableList.copyOf(properties);

        fixupName();
    }

    private void fixupName()
    {
        if (this.name.startsWith("m_"))
        {
            this.name = this.name.substring(2);
        }
    }

    private String calculateDefaultValue()
            throws ClassNotFoundException
    {
        if (type.equals("String")) {
            return "";
        }
        else {
            return String.valueOf(Defaults.defaultValue(ClassUtils.getClass(type)));

        }
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

    private class LabelComparator implements Comparator<AggregatorPropertyMetadata>
    {
        @Override
        public int compare(AggregatorPropertyMetadata o1, AggregatorPropertyMetadata o2)
        {
            return o1.getLabel().compareTo(o2.getLabel());
        }
    }

    private class ExplicitComparator implements Comparator<AggregatorPropertyMetadata>
    {
        private List<String> order;

        private ExplicitComparator(List<String> order)
        {
            this.order = order;
        }

        public int compare(AggregatorPropertyMetadata left, AggregatorPropertyMetadata right)
        {
            return Integer.compare(order.indexOf(left.getLabel()), order.indexOf(right.getLabel()));
        }
    }
}
