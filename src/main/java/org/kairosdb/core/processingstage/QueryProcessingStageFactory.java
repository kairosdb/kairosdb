package org.kairosdb.core.processingstage;

import com.google.common.collect.ImmutableList;
import org.kairosdb.core.processingstage.metadata.QueryProcessorMetadata;

public interface QueryProcessingStageFactory<QueryProcessorFamily>
{
    /**
     * Create new instance of a query processor.
     *
     * @param   name    name of the query processor
     * @return          created instance of the query processor
     */
    QueryProcessorFamily createQueryProcessor(String name);

    /**
     * Returns the query processor family class.
     *
     * @return          query processor family class
     */
    Class<QueryProcessorFamily> getQueryProcessorFamily();

    /**
     * Returns an {@link ImmutableList} of {@link QueryProcessorMetadata}
     * describing the query processing stage.
     *
     * @return the {@link ImmutableList} describing the processing chain
     */
    ImmutableList<QueryProcessorMetadata> getQueryProcessorMetadata();
}
