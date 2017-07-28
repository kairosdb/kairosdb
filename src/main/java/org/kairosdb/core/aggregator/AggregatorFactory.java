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

import com.google.inject.Inject;
import com.google.inject.Injector;
import org.kairosdb.core.annotation.QueryProcessingStage;
import org.kairosdb.core.processingstage.GenericQueryProcessingStageFactory;

import java.lang.reflect.InvocationTargetException;

@QueryProcessingStage(
        name = "aggregators",
        label = "Aggregator"
)
public class AggregatorFactory extends GenericQueryProcessingStageFactory<Aggregator>
{
    @Inject
    public AggregatorFactory(Injector injector)
            throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException
    {
        super(injector, Aggregator.class);
    }
}
