// OpenTSDB2
// Copyright (C) 2013 Proofpoint, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>
package net.opentsdb.testing;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.datastore.DataPointGroup;
import net.opentsdb.core.datastore.DataPointRow;

import java.util.*;

public class TestingDataPointRowImpl implements DataPointRow
{
	private List<DataPoint> dataPoints = new ArrayList<DataPoint>();
	private Iterator<DataPoint> iterator;
	private Map<String, String> tags = new TreeMap<String, String>();
	private String name;

	public void addTag(String name, String value)
	{
		tags.put(name, value);
	}

	public void setName(String name)
	{
		this.name = name;
	}

	@Override
	public String getName()
	{
		return (name);
	}

	@Override
	public Set<String> getTagNames()
	{
		return (tags.keySet());
	}

	@Override
	public String getTagValue(String tag)
	{
		return (tags.get(tag));
	}


	@Override
	public void close()
	{
	}

	public void addDataPoint(DataPoint dataPoint)
	{
		dataPoints.add(dataPoint);
	}

	@Override
	public boolean hasNext()
	{
		if (iterator == null)
			iterator = dataPoints.iterator();

		return (iterator.hasNext());
	}

	@Override
	public DataPoint next()
	{
		if (iterator == null)
			iterator = dataPoints.iterator();

		return (iterator.next());
	}

	@Override
	public void remove()
	{
	}
}