package org.kairosdb.metrics;

import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.StringDataPointFactory;
import org.kairosdb.eventbus.FilterEventBus;
import org.kairosdb.metrics4j.MetricSourceManager;
import org.kairosdb.metrics4j.SinkNotification;
import org.kairosdb.metrics4j.sinks.MetricSink;

import javax.inject.Inject;

public class InternalSinkSetup implements SinkNotification
{
	private final FilterEventBus m_eventBus;
	private final LongDataPointFactory m_longDataPointFactory;
	private final DoubleDataPointFactory m_doubleDataPointFactory;
	private final StringDataPointFactory m_stringDataPointFactory;

	@Inject
	public InternalSinkSetup(FilterEventBus eventBus, DoubleDataPointFactory doubleDataPointFactory,
			LongDataPointFactory longDataPointFactory, StringDataPointFactory stringDataPointFactory)
	{
		m_eventBus = eventBus;
		m_longDataPointFactory = longDataPointFactory;
		m_doubleDataPointFactory = doubleDataPointFactory;
		m_stringDataPointFactory = stringDataPointFactory;

		MetricSourceManager.getMetricConfig().getContext().registerSinkNotification(this);
	}


	@Override
	public void newSink(String name, MetricSink metricSink)
	{
		if (metricSink instanceof InternalSink)
		{
			((InternalSink)metricSink).initializeSink(m_eventBus, m_longDataPointFactory,
					m_doubleDataPointFactory, m_stringDataPointFactory);
		}
	}
}
