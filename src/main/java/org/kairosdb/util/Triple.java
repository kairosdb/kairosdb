package org.kairosdb.util;

/**
 Created by bhawkins on 5/15/14.
 */
public class Triple<F, S, T>
{
	private final F m_first;
	private final S m_second;
	private final T m_third;
	private final long m_time;

	public Triple(F first, S second, T third, long time)
	{
		m_first = first;
		m_second = second;
		m_third = third;
		m_time = time;
	}

	public F getFirst()
	{
		return m_first;
	}

	public S getSecond()
	{
		return m_second;
	}

	public T getThird()
	{
		return m_third;
	}

	public long getTime()
	{
		return m_time;
	}
}
