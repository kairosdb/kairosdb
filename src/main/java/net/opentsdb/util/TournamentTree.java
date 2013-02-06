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
package net.opentsdb.util;



import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;

public class TournamentTree<T>
	{
	private class TreeValue<T>
		{
		private int m_iteratorNum;
		private T m_value;
		private Iterator<T> m_iterator;
		
		public TreeValue(Iterator<T> iterator, T value, int iteratorNum)
			{
			m_iterator = iterator;
			m_value = value;
			m_iteratorNum = iteratorNum;
			}
			
		public int getIteratorNum() { return (m_iteratorNum); }
		public void setValue(T value) { m_value = value; }
		public T getValue() { return (m_value); }
		public Iterator<T> getIterator() { return (m_iterator); }
		}
		
	private class TreeComparator implements Comparator<TreeValue<T>>
		{
		public int compare(TreeValue<T> tv1, TreeValue<T> tv2)
			{
			int resp = m_comparator.compare(tv1.getValue(), tv2.getValue());
			
			if (resp == 0)
				return (tv1.getIteratorNum() - tv2.getIteratorNum());
			else
				return (resp);
			}
		}
	
	//===========================================================================
	private TreeSet<TreeValue<T>> m_treeSet;
	private Comparator<T> m_comparator;
	private int m_iteratorIndex = 0;
	
	public TournamentTree(Comparator<T> comparator)
		{
		m_comparator = comparator;
		m_treeSet = new TreeSet<TreeValue<T>>(new TreeComparator());
		}
		
	//---------------------------------------------------------------------------
	public void addIterator(Iterator<T> iterator)
		{
		if (iterator.hasNext())
			m_treeSet.add(new TreeValue<T>(iterator, iterator.next(), m_iteratorIndex ++));
		}
		
	//---------------------------------------------------------------------------
	public boolean hasNext()
	{
		return !m_treeSet.isEmpty();
	}

	//---------------------------------------------------------------------------
	public T nextElement()
	{
		TreeValue<T> value = m_treeSet.pollFirst();

		if (value == null)
			return (null);

		T ret = value.getValue();
		
		if (value.getIterator().hasNext())
		{
			value.setValue(value.getIterator().next());
			m_treeSet.add(value);
		}

		return (ret);
	}
}
