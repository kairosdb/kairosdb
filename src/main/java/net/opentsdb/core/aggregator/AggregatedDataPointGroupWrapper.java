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
package net.opentsdb.core.aggregator;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.datastore.DataPointGroup;

import java.util.Iterator;
import java.util.Map;

public abstract class AggregatedDataPointGroupWrapper extends DataPointGroup
{
	protected DataPoint currentDataPoint = null;
	private DataPointGroup innerDataPointGroup;


	public AggregatedDataPointGroupWrapper(DataPointGroup innerDataPointGroup)
	{
		super(innerDataPointGroup.getName(), innerDataPointGroup.getTags());

		this.innerDataPointGroup = innerDataPointGroup;
		if (innerDataPointGroup.hasNext())
			currentDataPoint = innerDataPointGroup.next();
	}

	@Override
	public boolean hasNext()
	{
		return currentDataPoint != null;
	}

	@Override
	public abstract DataPoint next();

	protected boolean hasNextInternal()
	{
		boolean hasNext = innerDataPointGroup.hasNext();
		if (!hasNext)
			currentDataPoint = null;
		return hasNext;
	}

	protected DataPoint nextInternal()
	{
		return innerDataPointGroup.next();
	}

	@Override
	public void remove()
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public void close()
	{
		innerDataPointGroup.close();
	}
}