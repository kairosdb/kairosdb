package net.opentsdb.core.aggregator;

import net.opentsdb.core.datastore.DataPointGroup;
import net.opentsdb.core.datastore.SortingDataPointGroup;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 3/1/13
 Time: 11:06 AM
 To change this template use File | Settings | File Templates.
 */
public abstract class SortedAggregator implements Aggregator
{
	@Override
	public DataPointGroup aggregate(List<DataPointGroup> listDataPointGroup)
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
