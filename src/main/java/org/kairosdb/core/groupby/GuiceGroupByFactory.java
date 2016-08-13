/*
 * Copyright 2013 Proofpoint Inc.
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
package org.kairosdb.core.groupby;

import com.google.inject.Binding;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.kairosdb.core.aggregator.annotation.GroupByName;

import java.util.HashMap;
import java.util.Map;

public class GuiceGroupByFactory implements GroupByFactory
{
	private Map<String, Class<? extends GroupBy>> groupBys = new HashMap<>();
	private Injector injector;


	@Inject
	public GuiceGroupByFactory(Injector injector)
	{
		this.injector = injector;
		Map<Key<?>, Binding<?>> bindings = injector.getAllBindings();

		for (Key<?> key : bindings.keySet())
		{
			Class<?> bindingClass = key.getTypeLiteral().getRawType();
			if (GroupBy.class.isAssignableFrom(bindingClass))
			{
				@SuppressWarnings("unchecked")
				Class<? extends GroupBy> castClass = (Class<? extends GroupBy>) bindingClass;
				GroupByName name = castClass.getAnnotation(GroupByName.class);
				if (name == null)
					throw new IllegalStateException("Aggregator class "+castClass.getName()+
							" does not have required annotation "+GroupByName.class.getName());

				groupBys.put(name.name(), castClass);
			}
		}
	}

	public GroupBy createGroupBy(String name)
	{
		Class<? extends GroupBy> groupByClass = groupBys.get(name);

		if (groupByClass == null)
			return (null);

		return injector.getInstance(groupByClass);
	}

}