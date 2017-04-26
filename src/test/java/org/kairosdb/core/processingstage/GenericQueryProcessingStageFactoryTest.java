package org.kairosdb.core.processingstage;

import com.google.common.base.Defaults;
import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.commons.lang3.ClassUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kairosdb.core.aggregator.Aggregator;
import org.kairosdb.core.aggregator.AggregatorFactory;
import org.kairosdb.core.aggregator.InvalidAggregator;
import org.kairosdb.core.annotatedAggregator.AAggregator;
import org.kairosdb.core.annotation.QueryCompoundProperty;
import org.kairosdb.core.annotation.QueryProperty;
import org.kairosdb.core.processingstage.metadata.QueryProcessorMetadata;
import org.kairosdb.core.processingstage.metadata.QueryPropertyMetadata;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang.StringUtils.isEmpty;
import static org.junit.Assert.assertEquals;
import static org.kairosdb.core.aggregator.GuiceAggregatorFactoryTest.assertProperty;

public class GenericQueryProcessingStageFactoryTest
{
    private static QueryProcessingStageFactory<Aggregator> factory;

    @BeforeClass
    public static void factory_generation_valid()
            throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException
    {
        Injector injector = Guice.createInjector((Module) binder -> binder.bind(AAggregator.class));
        GenericQueryProcessingStageFactoryTest.factory = new AggregatorFactory(injector);
    }

    @Test(expected = IllegalStateException.class)
    public void factory_generation_invalid_metadata()
            throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException
    {
        Injector injector = Guice.createInjector((Module) binder -> binder.bind(InvalidAggregator.class));
        QueryProcessingStageFactory<Aggregator> factory = new AggregatorFactory(injector);
    }

    @Test(expected = NullPointerException.class)
    public void factory_generation_invalid_injector()
            throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException
    {
        QueryProcessingStageFactory<?> factory = new AggregatorFactory(null);
    }

    @Test
    public void factory_getter_query_processor_family()
    {
        assertEquals("QueryProcessor family don't match", Aggregator.class, GenericQueryProcessingStageFactoryTest.factory.getQueryProcessorFamily());
    }

    @Test
    public void factory_getter_query_processor_metadata()
            throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException
    {
        assertQueryProcessors(
                ImmutableList.copyOf(factory_valid_metadata_generator()),
                GenericQueryProcessingStageFactoryTest.factory.getQueryProcessorMetadata()
        );
    }

    @Test
    public void factory_new_query_processor()
    {
        assertEquals("QueryProcessor created was invalid",
                AAggregator.class,
                GenericQueryProcessingStageFactoryTest.factory.createQueryProcessor("A").getClass());
    }



    static String getEnumAsString(Class type)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException
    {
        StringBuilder builder = new StringBuilder();
        Field[] declaredFields = type.getDeclaredFields();
        for (Field declaredField : declaredFields)
        {
            if (declaredField.isEnumConstant())
            {
                if (builder.length() > 0)
                    builder.append(',');
                builder.append(declaredField.getName());
            }
        }

        return builder.toString();
    }

    static String getType(Field field)
    {
        if (Collection.class.isAssignableFrom(field.getType()) || field.getType().isArray())
            return "array";
        return field.getType().getSimpleName();
    }

    static String getDefaultValue(Field field)
            throws ClassNotFoundException
    {
        if (field.getType().isAssignableFrom(String.class))
            return "";
        else if (Collection.class.isAssignableFrom(field.getType()) || field.getType().isArray())
            return "[]";
        else
            return String.valueOf(Defaults.defaultValue(ClassUtils.getClass(field.getType().getSimpleName())));
    }

    @SuppressWarnings("ConstantConditions")
    static List<QueryPropertyMetadata> getPropertyMetadata(Class clazz)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, ClassNotFoundException
    {
        checkNotNull(clazz, "class cannot be null");

        List<QueryPropertyMetadata> properties = new ArrayList<>();
        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields)
        {
            if (field.getAnnotation(QueryProperty.class) != null)
            {
                String type = getType(field);
                String options = null;
                if (field.getType().isEnum())
                {
                    options = getEnumAsString(field.getType());
                    type = "enum";
                }

                QueryProperty property = field.getAnnotation(QueryProperty.class);
                properties.add(new QueryPropertyMetadata(field.getName(), type, options,
                        isEmpty(property.default_value()) ? getDefaultValue(field) : property.default_value(),
                        property));
            }

            QueryCompoundProperty annotation = field.getAnnotation(QueryCompoundProperty.class);
            if (annotation != null)
            {
                properties.add(new QueryPropertyMetadata(field.getName(), annotation, getPropertyMetadata(field.getType())));
            }
        }

        if (clazz.getSuperclass() != null)
        {
            properties.addAll(getPropertyMetadata(clazz.getSuperclass()));
        }

        //noinspection Convert2Lambda
        properties.sort(new Comparator<QueryPropertyMetadata>()
        {
            @Override
            public int compare(QueryPropertyMetadata o1, QueryPropertyMetadata o2)
            {
                return o1.getLabel().compareTo(o2.getLabel());
            }
        });

        return properties;
    }

    static QueryProcessorMetadata[] factory_valid_metadata_generator()
            throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException
    {
        return new QueryProcessorMetadata[]{
                new QueryProcessorMetadata(
                        "A",
                        "A",
                        "The A Aggregator",
                        getPropertyMetadata(AAggregator.class)
                )
        };
    }

    static void assertQueryProcessors(ImmutableList<QueryProcessorMetadata> expectedQueryProcessorMetadatas,
                                      ImmutableList<QueryProcessorMetadata> actualQueryProcessorMetadatas)
    {
        assertEquals("QueryProcessor metadata quantity don't match", expectedQueryProcessorMetadatas.size(), actualQueryProcessorMetadatas.size());
        for (int i = 0; i < actualQueryProcessorMetadatas.size(); i++)
        {
            QueryProcessorMetadata expectedQueryProcessor = expectedQueryProcessorMetadatas.get(i);
            QueryProcessorMetadata actualQueryProcessorActual = actualQueryProcessorMetadatas.get(i);

            assertEquals("QueryProcessor metadata name don't match", expectedQueryProcessor.getName(), actualQueryProcessorActual.getName());
            assertEquals("QueryProcessor metadata description don't match", expectedQueryProcessor.getDescription(), actualQueryProcessorActual.getDescription());
            assertEquals("QueryProcessor metadata label don't match", expectedQueryProcessor.getLabel(), actualQueryProcessorActual.getLabel());
            assertQueryProperties(
                    expectedQueryProcessor.getProperties(),
                    actualQueryProcessorActual.getProperties()
            );
        }
    }

    static void assertQueryProperties(ImmutableList<QueryPropertyMetadata> expectedQueryPropertyMetadatas,
                                      ImmutableList<QueryPropertyMetadata> actualQueryPropertyMetadatas)
    {
        assertEquals("QueryProperty metadata quantity don't match", expectedQueryPropertyMetadatas.size(), actualQueryPropertyMetadatas.size());

        for (int i = 0; i < actualQueryPropertyMetadatas.size(); i++)
        {
            QueryPropertyMetadata expectedQueryProperty = expectedQueryPropertyMetadatas.get(i);
            QueryPropertyMetadata actualQueryProperty = actualQueryPropertyMetadatas.get(i);

            assertProperty(actualQueryProperty,
                    expectedQueryProperty.getName(), expectedQueryProperty.getLabel(), expectedQueryProperty.getDescription(),
                    expectedQueryProperty.getType(), expectedQueryProperty.getDefaultValue(),
                    expectedQueryProperty.getValidations());
        }
    }
}
