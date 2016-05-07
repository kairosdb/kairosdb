//
// DataPointListenerProvider.java
//
// Copyright 2013, NextPage Inc. All rights reserved.
//

package org.kairosdb.core;

import com.google.inject.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DataPointListenerProvider implements Provider<List<DataPointListener>>
{
	private List<DataPointListener> m_listeners = new ArrayList<DataPointListener>();

	@Inject
	public DataPointListenerProvider(Injector injector)
	{
		Map<Key<?>, Binding<?>> bindings = injector.getAllBindings();

		for (Key<?> key : bindings.keySet())
		{
			Class<?> bindingClass = key.getTypeLiteral().getRawType();
			if (DataPointListener.class.isAssignableFrom(bindingClass))
			{
				@SuppressWarnings("unchecked")
				Class<? extends DataPointListener> castClass = (Class<? extends DataPointListener>)bindingClass;
				m_listeners.add(injector.getInstance(castClass));
			}
		}
	}

	@Override
	public List<DataPointListener> get()
	{
		return (Collections.unmodifiableList(m_listeners));
	}
}
