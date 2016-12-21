//
// KairosMetricReporterListProvider.java
//
// Copyright 2013, NextPage Inc. All rights reserved.
//

package org.kairosdb.core.reporting;

import com.google.inject.*;

import java.util.*;

public class KairosMetricReporterListProvider implements Provider<Set<KairosMetricReporter>>
{
	private Set<KairosMetricReporter> m_reporters = new HashSet<KairosMetricReporter>();

	@Inject
	public KairosMetricReporterListProvider(Injector injector)
	{
		Map<Key<?>, Binding<?>> bindings = injector.getAllBindings();

		for (Key<?> key : bindings.keySet())
		{
			Class<?> bindingClass = key.getTypeLiteral().getRawType();
			if (KairosMetricReporter.class.isAssignableFrom(bindingClass))
			{
				KairosMetricReporter reporter = (KairosMetricReporter)injector.getInstance(bindingClass);
				m_reporters.add(reporter);
			}
		}
	}

	@Override
	public Set<KairosMetricReporter> get()
	{
		return (Collections.unmodifiableSet(m_reporters));
	}
}
