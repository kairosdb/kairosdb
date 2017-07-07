package org.kairosdb.core.http.rest;


import com.google.inject.Binding;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.kairosdb.core.http.rest.json.Query;
import org.kairosdb.plugin.QueryPreProcessor;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 Created by bhawkins on 6/12/17.
 */
public class GuiceQueryPreProcessor implements QueryPreProcessorContainer
{
	private List<QueryPreProcessor> m_preProcessors;


	@Inject
	public GuiceQueryPreProcessor(Injector injector)
	{
		m_preProcessors = new ArrayList<>();

		Map<Key<?>, Binding<?>> bindings = injector.getAllBindings();

		for (Key<?> key : bindings.keySet())
		{
			Class<?> bindingClass = key.getTypeLiteral().getRawType();
			if (QueryPreProcessor.class.isAssignableFrom(bindingClass))
			{
				m_preProcessors.add((QueryPreProcessor)injector.getInstance(bindingClass));
			}
		}
	}

	public Query preProcess(Query query)
	{
		Query ret = query;

		for (QueryPreProcessor preProcessor : m_preProcessors)
		{
			ret = preProcessor.preProcessQuery(ret);
		}

		return ret;
	}
}
