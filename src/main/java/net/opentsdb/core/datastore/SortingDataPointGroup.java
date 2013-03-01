package net.opentsdb.core.datastore;

import net.opentsdb.core.DataPoint;
import net.opentsdb.util.TournamentTree;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 3/1/13
 Time: 10:45 AM
 To change this template use File | Settings | File Templates.
 */
public class SortingDataPointGroup extends AbstractDataPointGroup
{
	private TournamentTree<DataPoint> m_tree;
	//We keep this list so we can close the iterators
	private List<DataPointGroup> m_taggedDataPointsList = new ArrayList<DataPointGroup>();

	public SortingDataPointGroup(String name)
	{
		super(name);
		m_tree = new TournamentTree<DataPoint>(new DataPointComparator());
	}

	public SortingDataPointGroup(String name, List<DataPointRow> listDataPointRow)
	{
		this(name);

		for (DataPointRow dataPoints : listDataPointRow)
		{
			addIterator(new DataPointGroupRowWrapper(dataPoints));
		}
	}

	public SortingDataPointGroup(List<DataPointGroup> listDataPointGroup)
	{
		this(listDataPointGroup.get(0).getName());

		for (DataPointGroup dataPoints : listDataPointGroup)
		{
			addIterator(dataPoints);
		}
	}


	public void addIterator(DataPointGroup taggedDataPoints)
	{
		m_tree.addIterator(taggedDataPoints);
		addTags(taggedDataPoints);
		m_taggedDataPointsList.add(taggedDataPoints);
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
		DataPoint ret = m_tree.nextElement();

		return ret;
	}


	private class DataPointComparator implements Comparator<DataPoint>
	{
		@Override
		public int compare(DataPoint point1, DataPoint point2)
		{
			return point1.compareTo(point2);
		}
	}
}
