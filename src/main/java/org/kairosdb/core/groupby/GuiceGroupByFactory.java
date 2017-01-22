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
package org.kairosdb.core.groupby;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binding;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.kairosdb.core.aggregator.json.QueryMetadata;
import org.kairosdb.core.aggregator.json.QueryPropertyMetadata;
import org.kairosdb.core.annotation.GroupByName;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.kairosdb.core.annotation.AnnotationUtils.getPropertyMetadata;

public class GuiceGroupByFactory implements GroupByFactory
{
	private Map<String, Class<GroupBy>> groupBys = new HashMap<String, Class<GroupBy>>();
	private Injector injector;
    private List<QueryMetadata> m_queryMetadata = new ArrayList<>();

	@Inject
	@SuppressWarnings("unchecked")
	public GuiceGroupByFactory(Injector injector)
            throws InvocationTargetException, NoSuchMethodException, ClassNotFoundException, IllegalAccessException
    {
		this.injector = injector;
		Map<Key<?>, Binding<?>> bindings = injector.getAllBindings();

		for (Key<?> key : bindings.keySet())
		{
			Class<?> bindingClass = key.getTypeLiteral().getRawType();
			if (GroupBy.class.isAssignableFrom(bindingClass))
			{
				GroupByName annotation = bindingClass.getAnnotation(GroupByName.class);
				if (annotation == null)
					throw new IllegalStateException("Aggregator class "+bindingClass.getName()+
							" does not have required annotation "+GroupByName.class.getName());

				groupBys.put(annotation.name(), (Class<GroupBy>)bindingClass);
                List<QueryPropertyMetadata> properties = getPropertyMetadata(bindingClass);
                m_queryMetadata.add(new QueryMetadata(annotation.name(), annotation.description(), properties));
            }
            Collections.sort(m_queryMetadata, new Comparator<QueryMetadata>()
            {
                @Override
                public int compare(QueryMetadata o1, QueryMetadata o2)
                {
                    return o1.getName().compareTo(o2.getName());
                }
            });
		}
	}

	public GroupBy createGroupBy(String name)
	{
		Class<GroupBy> groupByClass = groupBys.get(name);

		if (groupByClass == null)
			return (null);

		GroupBy groupBy = injector.getInstance(groupByClass);
		return (groupBy);
	}

    @Override
    public ImmutableList<QueryMetadata> getQueryMetadata()
    {
        return new ImmutableList.Builder<QueryMetadata>().addAll(m_queryMetadata).build();
    }

}