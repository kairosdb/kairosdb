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
        assertEquals("QueryProcessor family don't match", GenericQueryProcessingStageFactoryTest.factory.getQueryProcessorFamily(), Aggregator.class);
    }

    // Linked to QueryProcessorMetadata
//    @Test
    public void factory_getter_query_processor_metadata()
            throws ClassNotFoundException
    {
        assertQueryProcessors(GenericQueryProcessingStageFactoryTest.factory.getQueryProcessorMetadata(), ImmutableList.copyOf(factory_valid_metadata_generator()));
    }

    @Test
    public void factory_new_query_processor()
    {
        assertEquals("QueryProcessor created was invalid",
                GenericQueryProcessingStageFactoryTest.factory.createQueryProcessor("A").getClass(),
                AAggregator.class);
    }


    static QueryProcessorMetadata[] factory_valid_metadata_generator()
            throws ClassNotFoundException
    {
//        return new QueryProcessorMetadata[]{
//                new QueryProcessorMetadata(
//                        "allAnnotation",
//                        "AllAnnotation",
//                        "This is allAnnotation",
//                        new ArrayList<QueryPropertyMetadata>()
//                        {{
//                            add(new QueryPropertyMetadata("", "", "", "",
//                                    AAggregator.class.getAnnotation(QueryProcessor.class)));
//                        }}
//                )
//        };
        //TODO: How to generate/get QueryPropertyMetadata
        return null;
    }

    static void assertQueryProcessors(ImmutableList<QueryProcessorMetadata> queryProcessorMetadatas,
                                      ImmutableList<QueryProcessorMetadata> queryProcessorMetadatasBase)
    {
        assertEquals("QueryProcessor metadata size don't match", queryProcessorMetadatas.size(), queryProcessorMetadatasBase.size());
        for (int i = 0; i < queryProcessorMetadatas.size(); i++)
        {
            QueryProcessorMetadata queryProcessorMetadata = queryProcessorMetadatas.get(i);
            QueryProcessorMetadata queryProcessorBase = queryProcessorMetadatasBase.get(i);
            assertEquals("QueryProcessor metadata name don't match", queryProcessorMetadata.getName(), queryProcessorBase.getName());
            assertEquals("QueryProcessor metadata description don't match", queryProcessorMetadata.getDescription(), queryProcessorBase.getDescription());
            assertEquals("QueryProcessor metadata label don't match", queryProcessorMetadata.getLabel(), queryProcessorBase.getLabel());

            assertQueryProperties(queryProcessorMetadata.getProperties(), queryProcessorBase.getProperties());
        }
    }

    static void assertQueryProperties(ImmutableList<QueryPropertyMetadata> queryPropertyMetadatas,
                                      ImmutableList<QueryPropertyMetadata> queryPropertyMetadatasBase)
    {
        assertEquals("QueryProperty metadata size don't match", queryPropertyMetadatas.size(), queryPropertyMetadatasBase.size());

        for (int i = 0; i < queryPropertyMetadatas.size(); i++)
        {
            QueryPropertyMetadata queryPropertyMetadata = queryPropertyMetadatas.get(i);
            QueryPropertyMetadata queryPropertyBase = queryPropertyMetadatasBase.get(i);

            assertProperty(queryPropertyMetadata,
                    queryPropertyBase.getName(), queryPropertyBase.getLabel(), queryPropertyBase.getDescription(),
                    queryPropertyBase.getType(), queryPropertyBase.getDefaultValue(),
                    queryPropertyBase.getValidations());
        }
    }
}
