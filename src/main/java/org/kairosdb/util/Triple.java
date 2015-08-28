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
	private final int m_ttl;

	public Triple(F first, S second, T third, long time, int ttl)
	{
		m_first = first;
		m_second = second;
		m_third = third;
		m_time = time;
		m_ttl = ttl;
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

	public int getTtl()
	{
		return m_ttl;
	}
}
