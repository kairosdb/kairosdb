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
package org.kairosdb.core.aggregator;

import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datastore.DataPointGroup;

import java.util.Set;

public abstract class AggregatedDataPointGroupWrapper implements DataPointGroup
{
	protected DataPoint currentDataPoint = null;
	private DataPointGroup innerDataPointGroup;


	public AggregatedDataPointGroupWrapper(DataPointGroup innerDataPointGroup)
	{
		this.innerDataPointGroup = innerDataPointGroup;

		if (innerDataPointGroup.hasNext())
			currentDataPoint = innerDataPointGroup.next();
	}

	@Override
	public String getName()
	{
		return (innerDataPointGroup.getName());
	}

	@Override
	public Set<String> getTagNames()
	{
		return (innerDataPointGroup.getTagNames());
	}

	@Override
	public Set<String> getTagValues(String tag)
	{
		return (innerDataPointGroup.getTagValues(tag));
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