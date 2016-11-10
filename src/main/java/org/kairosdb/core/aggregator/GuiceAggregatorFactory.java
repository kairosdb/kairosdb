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
import com.google.common.collect.ImmutableList.Builder;
import com.google.inject.Binding;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.kairosdb.core.aggregator.annotation.AggregatorName;
import org.kairosdb.core.aggregator.annotation.AggregatorProperty;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GuiceAggregatorFactory implements AggregatorFactory
{
	private Map<String, Class<Aggregator>> m_aggregators = new HashMap<>();
    private List<AggregatorMetadata> m_aggregatorsMetadata = new ArrayList<>();
	private Injector m_injector;


	@Inject
	@SuppressWarnings("unchecked")
	public GuiceAggregatorFactory(Injector injector)
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
					throw new IllegalStateException("Aggregator class " + bindingClass.getName()+
							" does not have required annotation " + AggregatorName.class.getName());

				m_aggregators.put(ann.name(), (Class<Aggregator>)bindingClass);

                ImmutableList<AggregatorPropertyMetadata> properties = getAggregatorPropertyMetadata(ann);
                m_aggregatorsMetadata.add(new AggregatorMetadata(ann.name(), ann.description(), properties));
			}
		}
        m_aggregatorsMetadata.sort(new Comparator<AggregatorMetadata>()
        {
            @Override
            public int compare(AggregatorMetadata o1, AggregatorMetadata o2)
            {
                return o1.getName().compareTo(o2.getName());
            }
        });
	}

    private ImmutableList<AggregatorPropertyMetadata> getAggregatorPropertyMetadata(AggregatorName ann)
    {
        Builder<AggregatorPropertyMetadata> builder = new ImmutableList.Builder<>();
        for (AggregatorProperty aggregatorProperty : ann.properties()) {
            builder.add(new AggregatorPropertyMetadata(aggregatorProperty.name(), aggregatorProperty.type(), aggregatorProperty.values()));
        } return builder.build();
    }

    public Aggregator createAggregator(String name)
	{
		Class<Aggregator> aggClass = m_aggregators.get(name);

		if (aggClass == null)
			return (null);

        return (m_injector.getInstance(aggClass));
	}

    @Override
    public ImmutableList<AggregatorMetadata> getAggregatorMetadata()
    {
        return new ImmutableList.Builder<AggregatorMetadata>().addAll(m_aggregatorsMetadata).build();
    }

}
