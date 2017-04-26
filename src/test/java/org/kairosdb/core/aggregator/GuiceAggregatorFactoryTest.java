package org.kairosdb.core.aggregator;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.junit.Test;
import org.kairosdb.core.annotatedAggregator.AAggregator;
import org.kairosdb.core.processingstage.metadata.QueryProcessorMetadata;
import org.kairosdb.core.processingstage.metadata.QueryPropertyMetadata;
import org.kairosdb.core.processingstage.metadata.QueryValidationMetadata;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
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
        AggregatorFactory factory = new AggregatorFactory(injector);

        ImmutableList<QueryProcessorMetadata> queryMetadata = factory.getQueryProcessorMetadata();

        assertEquals(1, queryMetadata.size());
        QueryProcessorMetadata metadata = queryMetadata.get(0);
        assertEquals(metadata.getName(), "A");
        assertEquals(metadata.getDescription(), "The A Aggregator");

        assertThat(metadata.getProperties().size(), equalTo(8));
        ImmutableList<QueryPropertyMetadata> properties = metadata.getProperties();
        assertProperty(properties.get(0), "allAnnotation", "AllAnnotation", "This is allAnnotation", "int", "2",
                ImmutableList.copyOf(new QueryValidationMetadata[]{
                        new QueryValidationMetadata("value > 0", "js", "Value must be greater than 0.")
                }));
        assertProperty(properties.get(1), "inherited", "Inherited", "This is alpha", "int", "1", ImmutableList.copyOf(new ArrayList<>()));
        assertProperty(properties.get(2), "myBoolean", "MyBoolean", "This is myBoolean", "boolean", "false", ImmutableList.copyOf(new ArrayList<>()));
        assertProperty(properties.get(3), "myDouble", "MyDouble", "This is myDouble", "double", "0.0", ImmutableList.copyOf(new ArrayList<>()));
        assertProperty(properties.get(4), "myInt", "MyInt", "This is myInt", "int", "0", ImmutableList.copyOf(new ArrayList<>()));
        assertProperty(properties.get(5), "myLong", "MyLong", "This is myLong", "long", "0", ImmutableList.copyOf(new ArrayList<>()));
        assertProperty(properties.get(6), "myString", "MyString", "This is myString", "String", "", ImmutableList.copyOf(new ArrayList<>()));
        assertProperty(properties.get(7), "sampling", "Sampling");

        ImmutableList<QueryPropertyMetadata> samplingProperties = properties.get(7).getProperties();
        assertProperty(samplingProperties.get(0), "value", "Value", "The number of units for the aggregation buckets", "long", "1",
                ImmutableList.copyOf(new QueryValidationMetadata[]{
                        new QueryValidationMetadata("value > 0", "js", "Value must be greater than 0.")
                }));
        assertProperty(samplingProperties.get(1), "unit", "Unit", "The time unit for the sampling rate", "enum", "MILLISECONDS", ImmutableList.copyOf(new ArrayList<>()));
    }

    private static void assertProperty(QueryPropertyMetadata property, String name, String label)
    {
        assertEquals("Names don't match", property.getName(), name);
        assertEquals("Labels don't match", property.getLabel(), label);
    }

    public static void assertProperty(
            QueryPropertyMetadata property,
            String name, String label, String description, String type, Object defaultValue, ImmutableList<QueryValidationMetadata> validations)
    {
        assertEquals("Names don't match", property.getName(), name);
        assertEquals("Labels don't match", property.getLabel(), label);
        assertEquals("Descriptions don't match", property.getDescription(), description);
        assertEquals("Types don't match", property.getType(), type);
        assertEquals("Default values don't match", property.getDefaultValue(), defaultValue);
        assertValidations(property.getValidations(), validations);
    }

    private static void assertValidations(ImmutableList<QueryValidationMetadata> validations, ImmutableList<QueryValidationMetadata> comparators)
    {
        assertEquals("Validations number does not match", validations.size(), comparators.size());
        for (int i = 0; i < validations.size(); i++)
        {
            QueryValidationMetadata validation = validations.get(i);
            QueryValidationMetadata comparator = comparators.get(i);

            assertEquals("QueryValidationMetadata.expression does not match", validation.getExpression(), comparator.getExpression());
            assertEquals("QueryValidationMetadata.message does not match", validation.getMessage(), comparator.getMessage());
            assertEquals("QueryValidationMetadata.type does not match", validation.getType(), comparator.getType());
        }
    }
}