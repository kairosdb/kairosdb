package org.kairosdb.datastore.cassandra;

import com.google.inject.*;
import org.kairosdb.core.DataPointListener;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 Created by bhawkins on 11/7/14.
 */
public class RowKeyListenerProvider implements Provider<List<RowKeyListener>>
{
	private List<RowKeyListener> m_listeners = new ArrayList<RowKeyListener>();

	@Inject
	public RowKeyListenerProvider(Injector injector)
	{
		Map<Key<?>, Binding<?>> bindings = injector.getAllBindings();

		for (Key<?> key : bindings.keySet())
		{
			Class bindingClass = key.getTypeLiteral().getRawType();
			if (RowKeyListener.class.isAssignableFrom(bindingClass))
			{
				RowKeyListener listener = (RowKeyListener)injector.getInstance(bindingClass);
				m_listeners.add(listener);
			}
		}
	}

	@Override
	public List<RowKeyListener> get()
	{
		return (Collections.unmodifiableList(m_listeners));
	}
}
