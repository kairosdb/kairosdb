package org.kairosdb.core.datastore;

import org.kairosdb.core.DataPoint;
import org.kairosdb.core.groupby.GroupByResult;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 8/28/13
 Time: 1:19 PM
 To change this template use File | Settings | File Templates.
 */
public class EmptyDataPointGroup implements DataPointGroup
{
	private String m_name;
	private TagSet m_tags;

	public EmptyDataPointGroup(String name, TagSet tags)
	{
		m_name = name;
		m_tags = tags;
	}

	@Override
	public String getName()
	{
		return (m_name);
	}

	@Override
	public List<GroupByResult> getGroupByResult()
	{
		return (Collections.emptyList());
	}

	@Override
	public String getAPIDataType()
	{
		return null;
	}

	@Override
	public void close()
	{
	}

	@Override
	public boolean hasNext()
	{
		return false;
	}

	@Override
	public DataPoint next()
	{
		return null;
	}

	@Override
	public void remove()
	{
	}

	@Override
	public Set<String> getTagNames()
	{
		return (m_tags.getTagNames());
	}

	@Override
	public Set<String> getTagValues(String tag)
	{
		return (m_tags.getTagValues(tag));
	}
}
