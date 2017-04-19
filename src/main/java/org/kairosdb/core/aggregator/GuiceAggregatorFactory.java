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
import com.google.inject.Binding;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.kairosdb.core.aggregator.json.QueryMetadata;
import org.kairosdb.core.aggregator.json.QueryPropertyMetadata;
import org.kairosdb.core.annotation.AggregatorName;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.kairosdb.core.annotation.AnnotationUtils.getPropertyMetadata;


public class GuiceAggregatorFactory implements AggregatorFactory
{
    private Map<String, Class<Aggregator>> m_aggregators = new HashMap<>();
    private List<QueryMetadata> m_queryMetadata = new ArrayList<>();
    private Injector m_injector;


    @Inject
    @SuppressWarnings("unchecked")
    public GuiceAggregatorFactory(Injector injector)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, ClassNotFoundException
    {
        m_injector = injector;
        Map<Key<?>, Binding<?>> bindings = injector.getAllBindings();

        for (Key<?> key : bindings.keySet())
        {
            Class<?> bindingClass = key.getTypeLiteral().getRawType();
            if (Aggregator.class.isAssignableFrom(bindingClass))
            {
                AggregatorName ann = bindingClass.getAnnotation(AggregatorName.class);
                if (ann == null)
                {
                    throw new IllegalStateException("Aggregator class " + bindingClass.getName() +
                            " does not have required annotation " + AggregatorName.class.getName());
                }

                m_aggregators.put(ann.name(), (Class<Aggregator>) bindingClass);
                List<QueryPropertyMetadata> properties = getPropertyMetadata(bindingClass);
                m_queryMetadata.add(new QueryMetadata(ann.name(), labelizeAggregator(ann), ann.description(), properties));
            }
        }
        //noinspection Convert2Lambda
        m_queryMetadata.sort(new Comparator<QueryMetadata>()
        {
            @Override
            public int compare(QueryMetadata o1, QueryMetadata o2)
            {
                return o1.getName().compareTo(o2.getName());
            }
        });
    }

    public Aggregator createAggregator(String name)
    {
        Class<Aggregator> aggClass = m_aggregators.get(name);

        if (aggClass == null)
        {
            return (null);
        }

        return (m_injector.getInstance(aggClass));
    }

    @Override
    public ImmutableList<QueryMetadata> getQueryMetadata()
    {
        return new ImmutableList.Builder<QueryMetadata>().addAll(m_queryMetadata).build();
    }

    private String labelizeAggregator(AggregatorName annotation)
    {
        if (!annotation.label().isEmpty())
        {
            return annotation.label();
        }

        StringBuilder label = new StringBuilder();
        for (String word : annotation.name().toLowerCase().split("_"))
        {
            label.append(word.substring(0, 1).toUpperCase());
            label.append(word.substring(1));
            label.append(" ");
        }
        return label.toString().trim();
    }
}
