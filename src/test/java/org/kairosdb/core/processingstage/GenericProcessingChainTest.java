package org.kairosdb.core.processingstage;

import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kairosdb.core.aggregator.Aggregator;
import org.kairosdb.core.aggregator.AggregatorFactory;
import org.kairosdb.core.annotatedAggregator.AAggregator;
import org.kairosdb.core.groupby.GroupBy;
import org.kairosdb.core.processingstage.metadata.QueryProcessingStageMetadata;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.kairosdb.core.processingstage.GenericQueryProcessingStageFactoryTest.assertQueryProcessors;
import static org.kairosdb.core.processingstage.GenericQueryProcessingStageFactoryTest.factory_valid_metadata_generator;

public class GenericProcessingChainTest
{
    private static QueryProcessingChain processingChain;

    @BeforeClass
    public static void chain_generation_valid()
            throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException
    {
        Injector injector = Guice.createInjector((Module) binder -> binder.bind(AAggregator.class));

        GenericProcessingChainTest.processingChain = new TestKairosDBProcessingChain(new ArrayList<QueryProcessingStageFactory<?>>()
        {{
            add(new AggregatorFactory(injector));
        }});
    }

    @Test(expected = IllegalArgumentException.class)
    public void chain_generation_empty_list()
            throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException
    {
        new TestKairosDBProcessingChain(new ArrayList<>());
    }

    @Test(expected = NullPointerException.class)
    public void chain_generation_null_list()
    {
        new TestKairosDBProcessingChain(null);
    }

    @Test
    public void chain_getter_factory_with_name()
    {
        QueryProcessingStageFactory<?> factory = GenericProcessingChainTest.processingChain.getQueryProcessingStageFactory(Aggregator.class);
        assertEquals("Invalid type of QueryProcessingStageFactory", AggregatorFactory.class, factory.getClass());
    }

    @Test
    public void chain_getter_factory_with_name_failure()
    {
        QueryProcessingStageFactory<?> factory = GenericProcessingChainTest.processingChain.getQueryProcessingStageFactory(GroupBy.class);
        assertEquals("Invalid type of QueryProcessingStageFactory", null, factory);
    }

    @Test
    public void chain_getter_factory_with_class()
    {
        QueryProcessingStageFactory<?> factory = GenericProcessingChainTest.processingChain.getQueryProcessingStageFactory("aggregators");
        assertEquals("Invalid type of QueryProcessingStageFactory", AggregatorFactory.class, factory.getClass());
    }

    @Test
    public void chain_getter_factory_with_class_failure()
    {
        QueryProcessingStageFactory<?> factory = GenericProcessingChainTest.processingChain.getQueryProcessingStageFactory("groupby");
        assertEquals("Invalid type of QueryProcessingStageFactory", null, factory);
    }

    @Test
    public void chain_getter_metadata()
            throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException
    {
        assertQueryProcessorFactories(
                ImmutableList.copyOf(chain_valid_metadata_generator()),
                this.processingChain.getQueryProcessingChainMetadata()
        );
    }

    static QueryProcessingStageMetadata[] chain_valid_metadata_generator()
            throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException
    {
        return new QueryProcessingStageMetadata[]{
                new QueryProcessingStageMetadata(
                        "aggregators",
                        "Aggregator",
                        Arrays.asList(factory_valid_metadata_generator())
                )
        };
    }

    static void assertQueryProcessorFactories(ImmutableList<QueryProcessingStageMetadata> expectedProcessingChain,
                                              ImmutableList<QueryProcessingStageMetadata> actualProcessingChain)
    {
        assertEquals("QueryProcessingStage metadata size don't match", expectedProcessingChain.size(), actualProcessingChain.size());
        for (int i = 0; i < actualProcessingChain.size(); i++)
        {
            QueryProcessingStageMetadata expectedQueryProcessorStage = expectedProcessingChain.get(i);
            QueryProcessingStageMetadata actualQueryProcessorStage = actualProcessingChain.get(i);
            assertEquals("QueryProcessingStage metadata name don't match", expectedQueryProcessorStage.getName(), actualQueryProcessorStage.getName());
            assertEquals("QueryProcessingStage metadata label don't match", expectedQueryProcessorStage.getLabel(), actualQueryProcessorStage.getLabel());
            assertQueryProcessors(expectedQueryProcessorStage.getProperties(), actualQueryProcessorStage.getProperties());
        }
    }
}
