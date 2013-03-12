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
package org.kairosdb.util;



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
