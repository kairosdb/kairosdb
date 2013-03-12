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

import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.SortingDataPointGroup;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class SortedAggregator implements Aggregator
{
	@Override
	public DataPointGroup createAggregatorGroup(List<DataPointGroup> listDataPointGroup)
	{
		checkNotNull(listDataPointGroup);

		//No need to sort if there is only one.
		if (listDataPointGroup.size() == 1)
			return (aggregate(listDataPointGroup.get(0)));
		else
		{
			SortingDataPointGroup sorter = new SortingDataPointGroup(listDataPointGroup);

			return (aggregate(sorter));
		}
	}


	protected abstract DataPointGroup aggregate(DataPointGroup dataPointGroup);
}
