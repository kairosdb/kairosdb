package org.kairosdb.datastore.cassandra;

import java.util.Objects;

public class TimedString
{
	private final String m_string;
	private final long m_time;

	public TimedString(String string, long time)
	{
		m_string = string;
		m_time = time;
	}

	public String getString()
	{
		return m_string;
	}

	public long getTime()
	{
		return m_time;
	}

	@Override
	public boolean equals(Object o)
	{
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		TimedString that = (TimedString) o;
		return m_time == that.m_time &&
				Objects.equals(m_string, that.m_string);
	}

	@Override
	public int hashCode()
	{
		return Objects.hash(m_string, m_time);
	}
}
