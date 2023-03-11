package org.kairosdb.events;

import com.google.common.collect.ImmutableSortedMap;
import org.kairosdb.core.DataPoint;

import static java.util.Objects.requireNonNull;
import static org.kairosdb.util.Preconditions.requireNonNullOrEmpty;


/**
 Created by bhawkins on 9/17/16.

 Any listener that take a long time should use a separate thread
 to do any work.  The calling thread is the protocol thread.
 */
public class DataPointEvent
{
	private final String m_metricName;
	private final ImmutableSortedMap<String, String> m_tags;
	private final DataPoint m_dataPoint;
	private final int m_ttl;

	public DataPointEvent(String metricName, ImmutableSortedMap<String, String> tags, DataPoint dataPoint, int ttl)
	{
		m_metricName = requireNonNullOrEmpty(metricName);
		m_tags = requireNonNull(tags);
		m_dataPoint = requireNonNull(dataPoint);
		m_ttl = ttl;
	}

	public DataPointEvent(String metricName, ImmutableSortedMap<String, String> tags, DataPoint dataPoint)
	{
		m_metricName = requireNonNullOrEmpty(metricName);
		m_tags = requireNonNull(tags);
		m_dataPoint = requireNonNull(dataPoint);
		m_ttl = 0;
	}


	public String getMetricName()
	{
		return m_metricName;
	}

	public ImmutableSortedMap<String, String> getTags()
	{
		return m_tags;
	}

	public DataPoint getDataPoint()
	{
		return m_dataPoint;
	}

	public int getTtl()
	{
		return m_ttl;
	}

	@Override
	public boolean equals(Object o)
	{
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		DataPointEvent that = (DataPointEvent) o;

		if (m_ttl != that.m_ttl) return false;
		if (!m_metricName.equals(that.m_metricName)) return false;
		if (!m_tags.equals(that.m_tags)) return false;
		return m_dataPoint.equals(that.m_dataPoint);

	}

	@Override
	public int hashCode()
	{
		int result = m_metricName.hashCode();
		result = 31 * result + m_tags.hashCode();
		result = 31 * result + m_dataPoint.hashCode();
		result = 31 * result + m_ttl;
		return result;
	}

	@Override
	public String toString()
	{
		return "DataPointEvent{" +
				"m_metricName='" + m_metricName + '\'' +
				", m_tags=" + m_tags +
				", m_dataPoint=" + m_dataPoint +
				", m_ttl=" + m_ttl +
				'}';
	}
}
