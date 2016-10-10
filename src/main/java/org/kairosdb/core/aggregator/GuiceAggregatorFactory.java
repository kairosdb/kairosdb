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

import com.google.inject.Binding;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.kairosdb.core.aggregator.annotation.AggregatorName;

import java.util.*;

public class GuiceAggregatorFactory implements AggregatorFactory
{
	private Map<String, Class<Aggregator>> m_aggregators = new HashMap<>();
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
				AggregatorName ann = (AggregatorName)bindingClass.getAnnotation(AggregatorName.class);
				if (ann == null)
					throw new IllegalStateException("Aggregator class "+bindingClass.getName()+
							" does not have required annotation "+AggregatorName.class.getName());

				m_aggregators.put(ann.name(), (Class<Aggregator>)bindingClass);
			}
		}
	}

	public Aggregator createAggregator(String name)
	{
		Class<Aggregator> aggClass = m_aggregators.get(name);

		if (aggClass == null)
			return (null);

		Aggregator agg = m_injector.getInstance(aggClass);
		return (agg);
	}

}
