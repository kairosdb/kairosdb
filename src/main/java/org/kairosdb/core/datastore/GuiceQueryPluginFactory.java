package org.kairosdb.core.datastore;

import com.google.inject.Binding;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.kairosdb.core.annotation.PluginName;

import java.util.HashMap;
import java.util.Map;

/**
 Created by bhawkins on 11/23/14.
 */
public class GuiceQueryPluginFactory implements QueryPluginFactory
{
	private Map<String, Class<? extends QueryPlugin>> m_plugins = new HashMap<>();
	private final Injector m_injector;

	@Inject
	public GuiceQueryPluginFactory(Injector injector)
	{
		m_injector = injector;
		Map<Key<?>, Binding<?>> bindings = injector.getAllBindings();

		for (Key<?> key : bindings.keySet())
		{
			Class<?> bindingClass = key.getTypeLiteral().getRawType();
			if (QueryPlugin.class.isAssignableFrom(bindingClass))
			{
				@SuppressWarnings("unchecked")
				Class<? extends QueryPlugin> checkedClass = (Class<QueryPlugin>)bindingClass;
				
				PluginName ann = checkedClass.getAnnotation(PluginName.class);
				if (ann == null)
					throw new IllegalStateException("Aggregator class "+checkedClass.getName()+
							" does not have required annotation "+PluginName.class.getName());

				m_plugins.put(ann.name(), checkedClass);
			}
		}
	}

	@Override
	public QueryPlugin createQueryPlugin(String name)
	{
		Class<? extends QueryPlugin> pluginClass = m_plugins.get(name);

		if (pluginClass == null)
			return (null);

		QueryPlugin plugin = m_injector.getInstance(pluginClass);
		return (plugin);
	}
}
