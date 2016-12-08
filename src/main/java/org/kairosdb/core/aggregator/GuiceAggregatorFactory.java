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
import org.kairosdb.core.aggregator.annotation.AggregatorCompoundProperty;
import org.kairosdb.core.aggregator.annotation.AggregatorName;
import org.kairosdb.core.aggregator.annotation.AggregatorProperty;
import org.kairosdb.core.aggregator.json.AggregatorMetadata;
import org.kairosdb.core.aggregator.json.AggregatorPropertyMetadata;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
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
					throw new IllegalStateException("Aggregator class " + bindingClass.getName() +
							" does not have required annotation " + AggregatorName.class.getName());

				m_aggregators.put(ann.name(), (Class<Aggregator>) bindingClass);
				List<AggregatorPropertyMetadata> properties = getPropertyMetadata(new ArrayList<AggregatorPropertyMetadata>(), bindingClass);
				m_aggregatorsMetadata.add(new AggregatorMetadata(ann, properties));
			}
		}
		Collections.sort(m_aggregatorsMetadata, new Comparator<AggregatorMetadata>()
		{
			@Override
			public int compare(AggregatorMetadata o1, AggregatorMetadata o2)
			{
				return o1.getName().compareTo(o2.getName());
			}
		});
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

	private List<AggregatorPropertyMetadata> getPropertyMetadata(List<AggregatorPropertyMetadata> properties, Class type)
	{
		Field[] fields = type.getDeclaredFields();
		for (Field field : fields)
		{
			if (field.getAnnotation(AggregatorProperty.class) != null)
			{
				properties.add(new AggregatorPropertyMetadata(field.getAnnotation(AggregatorProperty.class)));
			}
			if (field.getAnnotation(AggregatorCompoundProperty.class) != null)
			{
				properties.add(new AggregatorPropertyMetadata(field.getAnnotation(AggregatorCompoundProperty.class)));
			}
		}

		if (type.getSuperclass() != null)
		{
			getPropertyMetadata(properties, type.getSuperclass());
		}

        Collections.sort(properties, new Comparator<AggregatorPropertyMetadata>()
        {
            @Override
            public int compare(AggregatorPropertyMetadata o1, AggregatorPropertyMetadata o2)
            {
                return o1.getLabel().compareTo(o2.getLabel());
            }
        });

		return properties;
	}
}
