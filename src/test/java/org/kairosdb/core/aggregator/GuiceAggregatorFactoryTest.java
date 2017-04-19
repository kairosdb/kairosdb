package org.kairosdb.core.aggregator;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.junit.Test;
import org.kairosdb.core.aggregator.json.QueryMetadata;
import org.kairosdb.core.aggregator.json.QueryPropertyMetadata;
import org.kairosdb.core.annotatedAggregator.AAggregator;

import java.lang.reflect.InvocationTargetException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class GuiceAggregatorFactoryTest
{
    @Test
    public void test_inherited()
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, ClassNotFoundException
    {
        Injector injector = Guice.createInjector(new Module()
        {
            @Override
            public void configure(Binder binder)
            {
                binder.bind(AAggregator.class);
            }
        });
        GuiceAggregatorFactory factory = new GuiceAggregatorFactory(injector);

        ImmutableList<QueryMetadata> queryMetadata = factory.getQueryMetadata();

        assertThat(1, equalTo(queryMetadata.size()));
        QueryMetadata metadata = queryMetadata.get(0);
        assertThat(metadata.getName(), equalTo("A"));
        assertThat(metadata.getDescription(), equalTo("The A Aggregator"));

        assertThat(metadata.getProperties().size(), equalTo(8));
        ImmutableList<QueryPropertyMetadata> properties = metadata.getProperties();
        assertProperty(properties.get(0), "allAnnotation", "AllAnnotation", "This is allAnnotation", "int", "2", "value > 0");
        assertProperty(properties.get(1), "inherited", "Inherited", "This is alpha", "int", "1", "");
        assertProperty(properties.get(2), "myBoolean", "MyBoolean", "This is myBoolean", "boolean", "false", "");
        assertProperty(properties.get(3), "myDouble", "MyDouble", "This is myDouble", "double", "0.0", "");
        assertProperty(properties.get(4), "myInt", "MyInt", "This is myInt", "int", "0", "");
        assertProperty(properties.get(5), "myLong", "MyLong", "This is myLong", "long", "0", "");
        assertProperty(properties.get(6), "myString", "MyString", "This is myString", "String", "", "");
        assertProperty(properties.get(7), "sampling", "Sampling");

        ImmutableList<QueryPropertyMetadata> samplingProperties = properties.get(7).getProperties();
        assertProperty(samplingProperties.get(0), "value", "Value", "The number of units for the aggregation buckets", "long", "1", "value > 0");
        assertProperty(samplingProperties.get(1), "unit", "Unit", "The time unit for the sampling rate", "enum", "MILLISECONDS", "");
    }

    private static void assertProperty(QueryPropertyMetadata property, String name, String label)
    {
        assertThat("Names don't match", property.getName(), equalTo(name));
        assertThat("Labels don't match", property.getLabel(), equalTo(label));
    }

    private static void assertProperty(
            QueryPropertyMetadata property,
            String name, String label, String description, String type, String defaultValue, String validation)
    {
        assertThat("Names don't match", property.getName(), equalTo(name));
        assertThat("Labels don't match", property.getLabel(), equalTo(label));
        assertThat("Descriptions don't match", property.getDescription(), equalTo(description));
        assertThat("Types don't match", property.getType(), equalTo(type));
        assertThat("Default values don't match", property.getDefaultValue(), equalTo(defaultValue));
        assertThat("ValidationProperty.java does not match", property.getValidation(), equalTo(validation));
    }
}