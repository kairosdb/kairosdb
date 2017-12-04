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
	private Map<String, Class<QueryPlugin>> m_plugins = new HashMap<String, Class<QueryPlugin>>();
	private final Injector m_injector;

	@Inject
	@SuppressWarnings("unchecked")
	public GuiceQueryPluginFactory(Injector injector)
	{
		m_injector = injector;
		Map<Key<?>, Binding<?>> bindings = injector.getAllBindings();

		for (Key<?> key : bindings.keySet())
		{
			Class<?> bindingClass = key.getTypeLiteral().getRawType();
			if (QueryPlugin.class.isAssignableFrom(bindingClass))
			{
				PluginName ann = (PluginName) bindingClass.getAnnotation(PluginName.class);
				if (ann == null)
					throw new IllegalStateException("QueryPlugin class " + bindingClass.getName() +
							" does not have required annotation " + PluginName.class.getName());

				m_plugins.put(ann.name(), (Class<QueryPlugin>)bindingClass);
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
