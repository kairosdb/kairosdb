package org.kairosdb.metrics;

import com.google.common.collect.ImmutableSortedMap;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.StringDataPointFactory;
import org.kairosdb.eventbus.FilterEventBus;
import org.kairosdb.eventbus.Publisher;
import org.kairosdb.events.DataPointEvent;
import org.kairosdb.metrics4j.MetricsContext;
import org.kairosdb.metrics4j.formatters.Formatter;
import org.kairosdb.metrics4j.internal.FormattedMetric;
import org.kairosdb.metrics4j.reporting.DoubleValue;
import org.kairosdb.metrics4j.reporting.LongValue;
import org.kairosdb.metrics4j.reporting.MetricValue;
import org.kairosdb.metrics4j.sinks.MetricSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

public class InternalSink implements MetricSink
{
	public static final Logger logger = LoggerFactory.getLogger(InternalSink.class);

	private Publisher<DataPointEvent> m_publisher;
	private LongDataPointFactory m_longDataPointFactory;
	private DoubleDataPointFactory m_doubleDataPointFactory;
	private StringDataPointFactory m_stringDataPointFactory;

	private Duration m_ttl = Duration.ofSeconds(0);

	@Override
	public void reportMetrics(List<FormattedMetric> metrics)
	{
		for (FormattedMetric metric : metrics)
		{
			String metricTtl = metric.getProps().get("ttl");
			int timeout = 0;
			if (metricTtl != null)
				timeout = Integer.valueOf(metricTtl);
			else
				timeout = (int)m_ttl.getSeconds();

			ImmutableSortedMap<String, String> tags = ImmutableSortedMap.copyOf(metric.getTags());
			for (FormattedMetric.Sample sample : metric.getSamples())
			{
				MetricValue value = sample.getValue();
				String type = value.getType();
				if (type.equals(MetricValue.TYPE_DOUBLE))
				{
					m_publisher.post(new DataPointEvent(sample.getMetricName(), tags,
							m_doubleDataPointFactory.createDataPoint(sample.getTime().toEpochMilli(), ((DoubleValue) value).getValue()), timeout));
				}
				else if (type.equals(MetricValue.TYPE_LONG))
				{
					m_publisher.post(new DataPointEvent(sample.getMetricName(), tags,
							m_longDataPointFactory.createDataPoint(sample.getTime().toEpochMilli(), ((LongValue) value).getValue()), timeout));
				}
				else if (type.equals(MetricValue.TYPE_STRING))
				{
					m_publisher.post(new DataPointEvent(sample.getMetricName(), tags,
							m_stringDataPointFactory.createDataPoint(sample.getTime().toEpochMilli(), value.getValueAsString()), timeout));
				}
				else
				{
					logger.warn("Unrecognized internal metric, {} of type {}", sample.getMetricName(), value.getType());
				}
			}

		}
	}

	public void setTtl(Duration ttl)
	{
		this.m_ttl = ttl;
	}

	@Override
	public Formatter getDefaultFormatter()
	{
		return null;
	}

	@Override
	public void init(MetricsContext context)
	{
	}

	public void initializeSink(FilterEventBus eventBus, LongDataPointFactory longDataPointFactory,
			DoubleDataPointFactory doubleDataPointFactory, StringDataPointFactory stringDataPointFactory)
	{
		m_publisher = eventBus.createPublisher(DataPointEvent.class);
		m_longDataPointFactory = longDataPointFactory;
		m_doubleDataPointFactory = doubleDataPointFactory;
		m_stringDataPointFactory = stringDataPointFactory;
	}
}
