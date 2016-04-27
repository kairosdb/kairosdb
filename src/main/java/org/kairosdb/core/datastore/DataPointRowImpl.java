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
package org.kairosdb.core.datastore;

import org.kairosdb.core.DataPoint;

import java.util.*;

public class DataPointRowImpl implements DataPointRow
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
	public String getDatastoreType()
	{
		return dataPoints.get(0).getDataStoreDataType();
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

	@Override
	public int getDataPointCount()
	{
		return dataPoints.size();
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