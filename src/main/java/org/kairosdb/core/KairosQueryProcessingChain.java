package org.kairosdb.core;

import com.google.inject.Inject;
import org.kairosdb.core.aggregator.Aggregator;
import org.kairosdb.core.groupby.GroupBy;
import org.kairosdb.core.processingstage.GenericQueryProcessingChain;
import org.kairosdb.core.processingstage.QueryProcessingStageFactory;

import java.util.Arrays;

public class KairosQueryProcessingChain extends GenericQueryProcessingChain
{
    @Inject
    public KairosQueryProcessingChain(QueryProcessingStageFactory<Aggregator> aggregatorFactory, QueryProcessingStageFactory<GroupBy> groupByFactory)
    {
        super(Arrays.asList(
                groupByFactory,
                aggregatorFactory
        ));
    }
}
