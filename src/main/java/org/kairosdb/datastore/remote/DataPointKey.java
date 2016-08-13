package org.kairosdb.datastore.remote;

import com.google.common.collect.ImmutableSortedMap;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 12/10/13
 Time: 8:25 AM
 To change this template use File | Settings | File Templates.
 */
public class DataPointKey
{
	private final String m_name;
	private final ImmutableSortedMap<String, String> m_tags;
	private final String m_type;
	private final int m_ttl;


	public DataPointKey(String name, ImmutableSortedMap<String, String> tags, String type, int ttl)
	{
		m_name = name;
		m_tags = tags;
		m_type = type;
		m_ttl  = ttl;
	}

	public String getName()
	{
		return m_name;
	}

	public ImmutableSortedMap<String, String> getTags()
	{
		return m_tags;
	}

	public String getType()
	{
		return m_type;
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

		DataPointKey that = (DataPointKey) o;

		if (m_ttl != that.m_ttl) return false;
		if (!m_name.equals(that.m_name)) return false;
		if (!m_tags.equals(that.m_tags)) return false;
		return m_type.equals(that.m_type);

	}

	@Override
	public int hashCode()
	{
		int result = m_name.hashCode();
		result = 31 * result + m_tags.hashCode();
		result = 31 * result + m_type.hashCode();
		result = 31 * result + m_ttl;
		return result;
	}
}
