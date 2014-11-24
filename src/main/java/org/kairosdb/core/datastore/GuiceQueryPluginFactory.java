package org.kairosdb.core.datastore;

import com.google.inject.Binding;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.kairosdb.core.aggregator.Aggregator;
import org.kairosdb.core.aggregator.annotation.AggregatorName;

import java.util.HashMap;
import java.util.Map;

/**
 Created by bhawkins on 11/23/14.
 */
public class GuiceQueryPluginFactory implements QueryPluginFactory
{
	private Map<String, Class<QueryPlugin>> m_plugins = new HashMap<String, Class<QueryPlugin>>();
	private final Injector m_injector;

	@Inject
	public GuiceQueryPluginFactory(Injector injector)
	{
		m_injector = injector;
		Map<Key<?>, Binding<?>> bindings = injector.getAllBindings();

		for (Key<?> key : bindings.keySet())
		{
			Class bindingClass = key.getTypeLiteral().getRawType();
			if (Aggregator.class.isAssignableFrom(bindingClass))
			{
				AggregatorName ann = (AggregatorName)bindingClass.getAnnotation(AggregatorName.class);
				if (ann == null)
					throw new IllegalStateException("Aggregator class "+bindingClass.getName()+
							" does not have required annotation "+AggregatorName.class.getName());

				m_plugins.put(ann.name(), bindingClass);
			}
		}
	}

	@Override
	public QueryPlugin createQueryPlugin(String name)
	{
		Class<QueryPlugin> pluginClass = m_plugins.get(name);

		if (pluginClass == null)
			return (null);

		QueryPlugin plugin = m_injector.getInstance(pluginClass);
		return (plugin);
	}
}
