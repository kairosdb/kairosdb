/*
 * Copyright 2013 Proofpoint Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.kairosdb.testing;

import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datastore.AbstractDataPointGroup;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ListDataPointGroup extends AbstractDataPointGroup
{
	private List<DataPoint> dataPoints = new ArrayList<DataPoint>();
	private Iterator<DataPoint> iterator;

	public ListDataPointGroup(String name)
	{
		super(name);
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
}