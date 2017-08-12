package org.kairosdb.core;

import com.google.inject.Inject;
import org.kairosdb.core.processingstage.FeatureProcessingFactory;
import org.kairosdb.core.processingstage.GenericFeatureProcessor;
import org.kairosdb.plugin.Aggregator;
import org.kairosdb.plugin.GroupBy;

import java.util.Arrays;

public class KairosFeatureProcessor extends GenericFeatureProcessor
{
    @Inject
    public KairosFeatureProcessor(FeatureProcessingFactory<Aggregator> aggregatorFactory, FeatureProcessingFactory<GroupBy> groupByFactory)
    {
        super(Arrays.asList(
                groupByFactory,
                aggregatorFactory
        ));
    }
}
