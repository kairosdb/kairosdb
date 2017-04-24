package org.kairosdb.core.processingstage;

import com.google.common.collect.ImmutableList;
import org.kairosdb.core.processingstage.metadata.QueryProcessingStageMetadata;

public interface QueryProcessingChain
{
    QueryProcessingStageFactory<?> getQueryProcessingStageFactory(Class<?> queryProcessorFamily);

    QueryProcessingStageFactory<?> getQueryProcessingStageFactory(String queryProcessorFamilyName);

    ImmutableList<QueryProcessingStageMetadata> getQueryProcessingChainMetadata();
}
