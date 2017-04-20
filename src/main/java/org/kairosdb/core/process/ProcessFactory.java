package org.kairosdb.core.process;

import com.google.common.collect.ImmutableList;
import org.kairosdb.core.aggregator.json.QueryMetadata;

public interface ProcessFactory<ProcessType>
{
    ProcessType createProcess(String name);

    ImmutableList<QueryMetadata> getQueryMetadata();
}
