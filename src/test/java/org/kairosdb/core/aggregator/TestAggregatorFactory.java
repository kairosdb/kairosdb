/*
 * Copyright 2016 KairosDB Authors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.kairosdb.core.aggregator;

import com.google.common.collect.ImmutableList;
import org.kairosdb.core.annotation.Feature;
import org.kairosdb.core.annotation.FeatureComponent;
import org.kairosdb.core.datapoints.DoubleDataPointFactoryImpl;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.core.processingstage.FeatureProcessingFactory;
import org.kairosdb.core.processingstage.metadata.FeatureProcessorMetadata;

import java.util.HashMap;
import java.util.Map;

@Feature(
        name = "aggregators",
        label = "Test Aggregator"
)
public class TestAggregatorFactory implements FeatureProcessingFactory<Aggregator>
{
    private Map<String, Aggregator> m_aggregators = new HashMap<String, Aggregator>();

    public TestAggregatorFactory() throws KairosDBException
    {
        addAggregator(new SumAggregator(new DoubleDataPointFactoryImpl()));
        addAggregator(new MinAggregator(new DoubleDataPointFactoryImpl()));
        addAggregator(new MaxAggregator(new DoubleDataPointFactoryImpl()));
        addAggregator(new AvgAggregator(new DoubleDataPointFactoryImpl()));
        addAggregator(new StdAggregator(new DoubleDataPointFactoryImpl()));
        addAggregator(new DivideAggregator(new DoubleDataPointFactoryImpl()));
        addAggregator(new PercentileAggregator(new DoubleDataPointFactoryImpl()));
        addAggregator(new FirstAggregator(new DoubleDataPointFactoryImpl()));
        addAggregator(new LastAggregator(new DoubleDataPointFactoryImpl()));
        addAggregator(new SaveAsAggregator(null));
        addAggregator(new TrimAggregator());
    }

    private void addAggregator(Aggregator agg)
    {
        String name = (agg.getClass().getAnnotation(FeatureComponent.class)).name();
        m_aggregators.put(name, agg);
    }

    @Override
    public Aggregator createFeatureProcessor(String name)
    {
        return (m_aggregators.get(name));
    }

    @Override
    public Class<Aggregator> getFeature()
    {
        return Aggregator.class;
    }

    @Override
    public ImmutableList<FeatureProcessorMetadata> getFeatureProcessorMetadata() { return ImmutableList.copyOf(new FeatureProcessorMetadata[]{});}
}
