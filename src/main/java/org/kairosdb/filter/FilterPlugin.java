package org.kairosdb.filter;

import com.google.common.collect.ImmutableList;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.KairosDBService;
import org.kairosdb.core.annotation.InjectProperty;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.core.reporting.KairosMetricReporter;
import org.kairosdb.eventbus.Subscribe;
import org.kairosdb.events.DataPointEvent;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

public class FilterPlugin implements KairosDBService, KairosMetricReporter
{
	private final LongDataPointFactory m_dataPointFactory;
	private Link m_filterChain = null;
	private Link m_lastLink = null;
	private AtomicInteger m_skippedMetrics = new AtomicInteger();

	@Inject
	public FilterPlugin(LongDataPointFactory dataPointFactory)
	{
		m_dataPointFactory = dataPointFactory;
	}

	public FilterPlugin()
	{
		this(new LongDataPointFactoryImpl());
	}

	@Inject
	@Named("HOSTNAME")
	private String m_hostName = "none";

	private void addLink(Link link)
	{
		if (m_filterChain == null)
		{
			m_filterChain = link;
			m_lastLink = link;
		}
		else
		{
			m_lastLink.setNext(link);
			m_lastLink = link;
		}
	}

	@InjectProperty(prop = "kairosdb.filter.list", optional = true)
	public void setList(List<String> list)
	{
		if (list.size() != 0)
		{
			Link link = new ExactLink(new HashSet<>(list));
			addLink(link);
		}
	}

	@InjectProperty(prop = "kairosdb.filter.prefix", optional = true)
	public void setPrefixList(List<String> list)
	{
		if (list.size() != 0)
		{
			Link link = new PrefixLink(new HashSet<>(list));
			addLink(link);
		}
	}

	@InjectProperty(prop = "kairosdb.filter.regex", optional = true)
	public void setRegexList(List<String> list)
	{
		if (list.size() != 0)
		{
			Link link = new RegexLink(new HashSet<>(list));
			addLink(link);
		}
	}

	@Subscribe
	public DataPointEvent filterDataPoint(DataPointEvent event)
	{
		return m_filterChain.filter(event);
	}

	@Override
	public void start() throws KairosDBException
	{
		//Make sure the chain always has an end link
		addLink(new EndLink());
	}

	@Override
	public void stop()
	{

	}

	@Override
	public List<DataPointSet> getMetrics(long now)
	{
		long skippedMetrics = m_skippedMetrics.getAndSet(0);
		ImmutableList.Builder<DataPointSet> ret = ImmutableList.builder();

		DataPointSet ds = new DataPointSet("kairosdb.filter.skipped_metrics");
		ds.addTag("host", m_hostName);
		ds.addDataPoint(m_dataPointFactory.createDataPoint(now, skippedMetrics));
		ret.add(ds);

		return ret.build();
	}

	private interface Link
	{
		DataPointEvent filter(DataPointEvent event);
		void setNext(Link link);
	}

	private class EndLink implements Link
	{
		@Override
		public DataPointEvent filter(DataPointEvent event)
		{
			return event;
		}

		@Override
		public void setNext(Link link)
		{
		}
	}

	private class ExactLink implements Link
	{
		private Link m_next;
		private final Set<String> m_filter;

		public ExactLink(Set<String> filter)
		{
			m_filter = filter;
		}

		@Override
		public DataPointEvent filter(DataPointEvent event)
		{
			if (m_filter.contains(event.getMetricName()))
			{
				m_skippedMetrics.incrementAndGet();
				return null;
			}
			else
				return m_next.filter(event);
		}

		@Override
		public void setNext(Link link)
		{
			m_next = link;
		}
	}

	private class PrefixLink implements Link
	{
		private Link m_next;
		private final String[] m_filter;

		public PrefixLink(Set<String> filter)
		{
			m_filter = new String[filter.size()];
			int count = 0;
			for (String prefix : filter)
			{
				m_filter[count] = prefix;
				count ++;
			}
		}

		@Override
		public DataPointEvent filter(DataPointEvent event)
		{
			String name = event.getMetricName();

			for (int i = 0; i < m_filter.length; i++ )
			{
				if (name.startsWith(m_filter[i]))
				{
					m_skippedMetrics.incrementAndGet();
					return null;
				}
			}

			return m_next.filter(event);
		}

		@Override
		public void setNext(Link link)
		{
			m_next = link;
		}
	}

	private class RegexLink implements Link
	{
		private Link m_next;
		private final Pattern[] m_filter;

		public RegexLink(Set<String> filter)
		{
			m_filter = new Pattern[filter.size()];
			int count = 0;

			for (String regex : filter)
			{
				m_filter[count] = Pattern.compile(regex);
				count ++;
			}
		}

		@Override
		public DataPointEvent filter(DataPointEvent event)
		{
			String name = event.getMetricName();

			for (int i = 0; i < m_filter.length; i++ )
			{
				if (m_filter[i].matcher(name).matches())
				{
					m_skippedMetrics.incrementAndGet();
					return null;
				}
			}

			return m_next.filter(event);
		}

		@Override
		public void setNext(Link link)
		{
			m_next = link;
		}
	}
}
