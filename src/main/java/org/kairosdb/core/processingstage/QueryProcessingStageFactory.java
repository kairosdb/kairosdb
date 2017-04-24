package org.kairosdb.core.processingstage;

import com.google.common.collect.ImmutableList;
import org.kairosdb.core.processingstage.metadata.QueryProcessorMetadata;

public interface QueryProcessingStageFactory<QueryProcessorFamily>
{
    QueryProcessorFamily createQueryProcessor(String name);

    Class<QueryProcessorFamily> getQueryProcessorFamily();

    ImmutableList<QueryProcessorMetadata> getQueryProcessorMetadata();
}
