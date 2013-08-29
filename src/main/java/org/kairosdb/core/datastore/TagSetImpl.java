package org.kairosdb.core.datastore;

import com.google.common.collect.TreeMultimap;

import java.util.Set;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 8/28/13
 Time: 12:38 PM
 To change this template use File | Settings | File Templates.
 */
public class TagSetImpl implements TagSet
{
	private TreeMultimap<String, String> m_tags = TreeMultimap.create();

	public void addTag(String name, String value)
	{
		m_tags.put(name, value);
	}

	@Override
	public Set<String> getTagNames()
	{
		return (m_tags.keySet());
	}

	@Override
	public Set<String> getTagValues(String tag)
	{
		return (m_tags.get(tag));
	}
}
