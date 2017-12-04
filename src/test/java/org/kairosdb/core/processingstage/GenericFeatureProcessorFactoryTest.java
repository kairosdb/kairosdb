package org.kairosdb.core.processingstage;

import com.google.common.base.Defaults;
import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.commons.lang3.ClassUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kairosdb.core.aggregator.AggregatorFactory;
import org.kairosdb.core.aggregator.InvalidAggregator;
import org.kairosdb.core.annotatedAggregator.AAggregator;
import org.kairosdb.core.annotation.FeatureCompoundProperty;
import org.kairosdb.core.annotation.FeatureProperty;
import org.kairosdb.core.processingstage.metadata.FeatureProcessorMetadata;
import org.kairosdb.core.processingstage.metadata.FeaturePropertyMetadata;
import org.kairosdb.plugin.Aggregator;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.junit.Assert.assertEquals;
import static org.kairosdb.core.aggregator.GuiceAggregatorFactoryTest.assertProperty;

public class GenericFeatureProcessorFactoryTest
{
    private static FeatureProcessingFactory<Aggregator> factory;

    @BeforeClass
    public static void factory_generation_valid()
            throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException
    {
        Injector injector = Guice.createInjector((Module) binder -> binder.bind(AAggregator.class));
        GenericFeatureProcessorFactoryTest.factory = new AggregatorFactory(injector);
    }

    @Test(expected = IllegalStateException.class)
    public void factory_generation_invalid_metadata()
            throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException
    {
        Injector injector = Guice.createInjector((Module) binder -> binder.bind(InvalidAggregator.class));
        FeatureProcessingFactory<Aggregator> factory = new AggregatorFactory(injector);
    }

    @Test(expected = NullPointerException.class)
    public void factory_generation_invalid_injector()
            throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException
    {
        FeatureProcessingFactory<?> factory = new AggregatorFactory(null);
    }

    @Test
    public void factory_getter_query_processor_family()
    {
        assertEquals("FeatureComponent family don't match", Aggregator.class, GenericFeatureProcessorFactoryTest.factory.getFeature());
    }

    @Test
    public void factory_getter_query_processor_metadata()
            throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException
    {
        assertQueryProcessors(
                ImmutableList.copyOf(factory_valid_metadata_generator()),
                GenericFeatureProcessorFactoryTest.factory.getFeatureProcessorMetadata()
        );
    }

    @Test
    public void factory_new_query_processor()
    {
        assertEquals("FeatureComponent created was invalid",
                AAggregator.class,
                GenericFeatureProcessorFactoryTest.factory.createFeatureProcessor("A").getClass());
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
    static List<FeaturePropertyMetadata> getPropertyMetadata(Class clazz)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, ClassNotFoundException
    {
        checkNotNull(clazz, "class cannot be null");

        List<FeaturePropertyMetadata> properties = new ArrayList<>();
        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields)
        {
            if (field.getAnnotation(FeatureProperty.class) != null)
            {
                String type = getType(field);
                String options = null;
                if (field.getType().isEnum())
                {
                    options = getEnumAsString(field.getType());
                    type = "enum";
                }

                FeatureProperty property = field.getAnnotation(FeatureProperty.class);
                properties.add(new FeaturePropertyMetadata(field.getName(), type, options,
                        isEmpty(property.default_value()) ? getDefaultValue(field) : property.default_value(),
                        property));
            }

            FeatureCompoundProperty annotation = field.getAnnotation(FeatureCompoundProperty.class);
            if (annotation != null)
            {
                properties.add(new FeaturePropertyMetadata(field.getName(), annotation, getPropertyMetadata(field.getType())));
            }
        }

        if (clazz.getSuperclass() != null)
        {
            properties.addAll(getPropertyMetadata(clazz.getSuperclass()));
        }

        //noinspection Convert2Lambda
        properties.sort(new Comparator<FeaturePropertyMetadata>()
        {
            @Override
            public int compare(FeaturePropertyMetadata o1, FeaturePropertyMetadata o2)
            {
                return o1.getLabel().compareTo(o2.getLabel());
            }
        });

        return properties;
    }

    static FeatureProcessorMetadata[] factory_valid_metadata_generator()
            throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException
    {
        return new FeatureProcessorMetadata[]{
                new FeatureProcessorMetadata(
                        "A",
                        "A",
                        "The A Aggregator",
                        getPropertyMetadata(AAggregator.class)
                )
        };
    }

    static void assertQueryProcessors(ImmutableList<FeatureProcessorMetadata> expectedFeatureProcessorMetadata,
                                      ImmutableList<FeatureProcessorMetadata> actualFeatureProcessorMetadata)
    {
        assertEquals("FeatureComponent metadata quantity don't match", expectedFeatureProcessorMetadata.size(), actualFeatureProcessorMetadata.size());
        for (int i = 0; i < actualFeatureProcessorMetadata.size(); i++)
        {
            FeatureProcessorMetadata expectedQueryProcessor = expectedFeatureProcessorMetadata.get(i);
            FeatureProcessorMetadata actualQueryProcessorActual = actualFeatureProcessorMetadata.get(i);

            assertEquals("FeatureComponent metadata name don't match", expectedQueryProcessor.getName(), actualQueryProcessorActual.getName());
            assertEquals("FeatureComponent metadata description don't match", expectedQueryProcessor.getDescription(), actualQueryProcessorActual.getDescription());
            assertEquals("FeatureComponent metadata label don't match", expectedQueryProcessor.getLabel(), actualQueryProcessorActual.getLabel());
            assertQueryProperties(
                    expectedQueryProcessor.getProperties(),
                    actualQueryProcessorActual.getProperties()
            );
        }
    }

    static void assertQueryProperties(ImmutableList<FeaturePropertyMetadata> expectedFeaturePropertyMetadata,
                                      ImmutableList<FeaturePropertyMetadata> actualFeaturePropertyMetadata)
    {
        assertEquals("FeatureProperty metadata quantity don't match", expectedFeaturePropertyMetadata.size(), actualFeaturePropertyMetadata.size());

        for (int i = 0; i < actualFeaturePropertyMetadata.size(); i++)
        {
            FeaturePropertyMetadata expectedQueryProperty = expectedFeaturePropertyMetadata.get(i);
            FeaturePropertyMetadata actualQueryProperty = actualFeaturePropertyMetadata.get(i);

            assertProperty(actualQueryProperty,
                    expectedQueryProperty.getName(), expectedQueryProperty.getLabel(), expectedQueryProperty.getDescription(),
                    expectedQueryProperty.getType(), expectedQueryProperty.getDefaultValue(),
                    expectedQueryProperty.getValidations());
        }
    }
}
