//
// KairosMetricReporterListProvider.java
//
// Copyright 2013, NextPage Inc. All rights reserved.
//

package org.kairosdb.core.reporting;

import com.google.inject.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class KairosMetricReporterListProvider implements Provider<List<KairosMetricReporter>>
{
	private List<KairosMetricReporter> m_reporters = new ArrayList<KairosMetricReporter>();

	@Inject
	public KairosMetricReporterListProvider(Injector injector)
	{
		Map<Key<?>, Binding<?>> bindings = injector.getAllBindings();

		for (Key<?> key : bindings.keySet())
		{
			Class<?> bindingClass = key.getTypeLiteral().getRawType();
			if (KairosMetricReporter.class.isAssignableFrom(bindingClass))
			{
				@SuppressWarnings("unchecked")
				Class<? extends KairosMetricReporter> castClass = (Class<? extends KairosMetricReporter>)bindingClass;
				m_reporters.add(injector.getInstance(castClass));
			}
		}
	}

	@Override
	public List<KairosMetricReporter> get()
	{
		return (Collections.unmodifiableList(m_reporters));
	}
}
