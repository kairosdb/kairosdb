package org.kairosdb.core;

import com.google.inject.Inject;
import org.kairosdb.core.aggregator.Aggregator;
import org.kairosdb.core.groupby.GroupBy;
import org.kairosdb.core.processingstage.FeatureProcessingFactory;
import org.kairosdb.core.processingstage.GenericFeatureProcessor;

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
