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
import org.kairosdb.core.groupby.GroupByResult;
import org.kairosdb.util.TournamentTree;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class SortingDataPointGroup extends AbstractDataPointGroup
{
	private TournamentTree<DataPoint> m_tree;
	//We keep this list so we can close the iterators
	private List<DataPointGroup> m_taggedDataPointsList = new ArrayList<DataPointGroup>();

	public SortingDataPointGroup(String name, Order order)
	{
		super(name);
		m_tree = new TournamentTree<DataPoint>(new DataPointComparator(), order);
	}

	public SortingDataPointGroup(List<DataPointGroup> listDataPointGroup, Order order)
	{
		this(listDataPointGroup, null, order);
	}

	public SortingDataPointGroup(List<DataPointGroup> listDataPointGroup, GroupByResult groupByResult,
			Order order)
	{
		this(listDataPointGroup.size() == 0 ? "" : listDataPointGroup.get(0).getName(), order);

		if (groupByResult != null)
			addGroupByResult(groupByResult);

		for (DataPointGroup dataPointGroup : listDataPointGroup)
		{
			addIterator(dataPointGroup);
		}
	}

	public void addIterator(DataPointGroup taggedDataPoints)
	{
		m_tree.addIterator(taggedDataPoints);
		addTags(taggedDataPoints);
		m_taggedDataPointsList.add(taggedDataPoints);
	}

	@Override
	public String getAPIDataType()
	{
		return null;  //To change body of implemented methods use File | Settings | File Templates.
	}

	@Override
	public void close()
	{
		for (DataPointGroup taggedDataPoints : m_taggedDataPointsList)
		{
			taggedDataPoints.close();
		}
	}

	@Override
	public boolean hasNext()
	{
		return m_tree.hasNext();
	}

	@Override
	public DataPoint next()
	{
		return m_tree.nextElement();
	}


	private class DataPointComparator implements Comparator<DataPoint>
	{
		@Override
		public int compare(DataPoint point1, DataPoint point2)
		{
			long ret = point1.getTimestamp() - point2.getTimestamp();

			if (ret == 0L)
			{  //Simple hack to break a tie.
				ret = System.identityHashCode(point1) - System.identityHashCode(point2);
			}

			return (ret < 0L ? -1 : 1);
		}
	}
}
