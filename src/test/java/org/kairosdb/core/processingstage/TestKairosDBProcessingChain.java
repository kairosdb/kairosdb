package org.kairosdb.core.processingstage;

import java.util.List;

public class TestKairosDBProcessingChain extends GenericQueryProcessingChain
{
    public TestKairosDBProcessingChain(List<QueryProcessingStageFactory<?>> processingChain)
    {
        super(processingChain);
    }
}
