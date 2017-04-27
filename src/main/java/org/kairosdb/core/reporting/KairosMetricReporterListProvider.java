//
// KairosMetricReporterListProvider.java
//
// Copyright 2013, NextPage Inc. All rights reserved.
//

package org.kairosdb.core.reporting;

import com.google.inject.*;
import com.google.inject.spi.InjectionListener;
import com.google.inject.spi.TypeEncounter;
import com.google.inject.spi.TypeListener;

import java.util.*;

public class KairosMetricReporterListProvider implements TypeListener
{
	private Set<KairosMetricReporter> m_reporters = new HashSet<KairosMetricReporter>();

	public Set<KairosMetricReporter> get()
	{
		return (Collections.unmodifiableSet(m_reporters));
	}

	@Override
	public <I> void hear(TypeLiteral<I> type, TypeEncounter<I> encounter)
	{
		encounter.register(new InjectionListener<I>()
		{
			@Override
			public void afterInjection(I injectee)
			{
				if (KairosMetricReporter.class.isAssignableFrom(injectee.getClass()))
				{
					m_reporters.add((KairosMetricReporter)injectee);
				}
			}
		});
	}
}
