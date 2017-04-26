package org.kairosdb.core.processingstage;

import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kairosdb.core.aggregator.Aggregator;
import org.kairosdb.core.aggregator.AggregatorFactory;
import org.kairosdb.core.aggregator.InvalidAggregator;
import org.kairosdb.core.annotatedAggregator.AAggregator;
import org.kairosdb.core.processingstage.metadata.QueryProcessorMetadata;
import org.kairosdb.core.processingstage.metadata.QueryPropertyMetadata;

import java.lang.reflect.InvocationTargetException;

import static org.junit.Assert.assertEquals;
import static org.kairosdb.core.aggregator.GuiceAggregatorFactoryTest.assertProperty;
import static org.kairosdb.core.annotation.AnnotationUtils.getPropertyMetadata;

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
