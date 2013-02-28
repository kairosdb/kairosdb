//
// DataPointGroupRowWrapper.java
//
// Copyright 2013, NextPage Inc. All rights reserved.
//

package net.opentsdb.core.datastore;

import net.opentsdb.core.DataPoint;

import java.util.Collections;
import java.util.Set;

public class DataPointGroupRowWrapper implements DataPointGroup
{
	DataPointRow m_row;

	public DataPointGroupRowWrapper(DataPointRow row)
	{
		m_row = row;
	}


	@Override
	public String getName()
	{
		return (m_row.getName());
	}

	@Override
	public Set<String> getTagNames()
	{
		return (m_row.getTagNames());
	}

	@Override
	public Set<String> getTagValues(String tag)
	{
		return (Collections.singleton(m_row.getTagValue(tag)));
	}

	@Override
	public void close()
	{
		m_row.close();
	}

	@Override
	public boolean hasNext()
	{
		return (m_row.hasNext());
	}

	@Override
	public DataPoint next()
	{
		return (m_row.next());
	}

	@Override
	public void remove()
	{
		m_row.remove();
	}
}
