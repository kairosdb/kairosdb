//
// DataPointListenerProvider.java
//
// Copyright 2013, NextPage Inc. All rights reserved.
//

package org.kairosdb.core;

import com.google.inject.*;
import org.kairosdb.core.aggregator.Aggregator;
import org.kairosdb.core.aggregator.annotation.AggregatorName;

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
			Class bindingClass = key.getTypeLiteral().getRawType();
			if (DataPointListener.class.isAssignableFrom(bindingClass))
			{
				DataPointListener listener = (DataPointListener)injector.getInstance(bindingClass);
				m_listeners.add(listener);
			}
		}
	}

	@Override
	public List<DataPointListener> get()
	{
		return (Collections.unmodifiableList(m_listeners));
	}
}
