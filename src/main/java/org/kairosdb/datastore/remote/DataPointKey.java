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
	private final String m_key;
	private final String m_name;
	private final ImmutableSortedMap<String, String> m_tags;
	private final String m_type;


	public DataPointKey(String name, ImmutableSortedMap<String, String> tags, String type)
	{
		m_name = name;
		m_tags = tags;
		m_type = type;

		StringBuilder sb = new StringBuilder();
		sb.append(name).append(type);
		for (String key : tags.keySet())
		{
			sb.append(":").append(key).append("=").append(tags.get(key));
		}

		m_key = sb.toString();
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

	@Override
	public String toString()
	{
		return "DataPointKey{" +
				"m_key='" + m_key + '\'' +
				", m_name='" + m_name + '\'' +
				", m_tags=" + m_tags +
				", m_type='" + m_type + '\'' +
				'}';
	}
}
