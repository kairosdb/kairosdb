/*
 * Copyright 2016 KairosDB Authors
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


import org.junit.Test;
import org.kairosdb.core.datastore.Order;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.*;

public class TournamentTreeTest
{
	private class RowData
	{
		private long m_ts;
		private int m_value;

		public RowData(long ts, int value)
		{
			m_ts = ts;
			m_value = value;
		}

		public long getTimeStamp()
		{
			return (m_ts);
		}

		public int getValue()
		{
			return (m_value);
		}
	}

	private class RowDataComparator implements Comparator<RowData>
	{
		public int compare(RowData rd1, RowData rd2)
		{
			return ((int) (rd1.getTimeStamp() - rd2.getTimeStamp()));
		}
	}


	@Test
	public void testTwoIterators()
	{
		TournamentTree<RowData> tt = new TournamentTree<>(new RowDataComparator(), Order.ASC);

		List<RowData> list1 = new ArrayList<>();
		list1.add(new RowData(1, 0));
		list1.add(new RowData(2, 0));
		list1.add(new RowData(3, 0));
		list1.add(new RowData(4, 0));

		List<RowData> list2 = new ArrayList<>();
		list2.add(new RowData(5, 0));
		list2.add(new RowData(6, 0));
		list2.add(new RowData(7, 0));
		list2.add(new RowData(8, 0));

		List<RowData> list3 = new ArrayList<>();
		list3.add(new RowData(9, 0));
		list3.add(new RowData(10, 0));
		list3.add(new RowData(11, 0));
		list3.add(new RowData(12, 0));

		tt.addIterator(list1.iterator());
		tt.addIterator(list2.iterator());
		tt.addIterator(list3.iterator());

		assertEquals(1, tt.nextElement().getTimeStamp());
		assertEquals(2, tt.nextElement().getTimeStamp());
		assertEquals(3, tt.nextElement().getTimeStamp());
		assertEquals(4, tt.nextElement().getTimeStamp());
		assertEquals(5, tt.nextElement().getTimeStamp());
		assertEquals(6, tt.nextElement().getTimeStamp());
		assertEquals(7, tt.nextElement().getTimeStamp());
		assertEquals(8, tt.nextElement().getTimeStamp());
		assertEquals(9, tt.nextElement().getTimeStamp());
		assertEquals(10, tt.nextElement().getTimeStamp());
		assertEquals(11, tt.nextElement().getTimeStamp());
		assertEquals(12, tt.nextElement().getTimeStamp());
		assertFalse(tt.hasNext());
		assertNull(tt.nextElement());
	}

	@Test
	public void testTwoIteratorsSameData()
	{
		TournamentTree<RowData> tt = new TournamentTree<>(new RowDataComparator(), Order.ASC);

		List<RowData> list1 = new ArrayList<>();
		list1.add(new RowData(1, 0));
		list1.add(new RowData(3, 0));
		list1.add(new RowData(5, 0));
		list1.add(new RowData(7, 0));

		List<RowData> list2 = new ArrayList<>();
		list2.add(new RowData(1, 0));
		list2.add(new RowData(3, 0));
		list2.add(new RowData(5, 0));
		list2.add(new RowData(7, 0));

		tt.addIterator(list1.iterator());
		tt.addIterator(list2.iterator());

		assertEquals(1, tt.nextElement().getTimeStamp());
		assertEquals(1, tt.nextElement().getTimeStamp());
		assertEquals(3, tt.nextElement().getTimeStamp());
		assertEquals(3, tt.nextElement().getTimeStamp());
		assertEquals(5, tt.nextElement().getTimeStamp());
		assertEquals(5, tt.nextElement().getTimeStamp());
		assertEquals(7, tt.nextElement().getTimeStamp());
		assertEquals(7, tt.nextElement().getTimeStamp());
		assertFalse(tt.hasNext());
		assertNull(tt.nextElement());
	}


	@Test
	public void testThreeIterators()
	{
		TournamentTree<RowData> tt = new TournamentTree<>(new RowDataComparator(), Order.ASC);

		List<RowData> list1 = new ArrayList<>();
		list1.add(new RowData(1, 0));
		list1.add(new RowData(3, 0));
		list1.add(new RowData(5, 0));
		list1.add(new RowData(7, 0));

		List<RowData> list2 = new ArrayList<>();
		list2.add(new RowData(1, 0));
		list2.add(new RowData(2, 0));
		list2.add(new RowData(3, 0));
		list2.add(new RowData(4, 0));

		List<RowData> list3 = new ArrayList<>();
		list3.add(new RowData(2, 0));
		list3.add(new RowData(4, 0));
		list3.add(new RowData(6, 0));
		list3.add(new RowData(8, 0));

		tt.addIterator(list1.iterator());
		tt.addIterator(list2.iterator());
		tt.addIterator(list3.iterator());

		assertEquals(1, tt.nextElement().getTimeStamp());
		assertEquals(1, tt.nextElement().getTimeStamp());
		assertEquals(2, tt.nextElement().getTimeStamp());
		assertEquals(2, tt.nextElement().getTimeStamp());
		assertEquals(3, tt.nextElement().getTimeStamp());
		assertEquals(3, tt.nextElement().getTimeStamp());
		assertEquals(4, tt.nextElement().getTimeStamp());
		assertEquals(4, tt.nextElement().getTimeStamp());
		assertEquals(5, tt.nextElement().getTimeStamp());
		assertEquals(6, tt.nextElement().getTimeStamp());
		assertEquals(7, tt.nextElement().getTimeStamp());
		assertEquals(8, tt.nextElement().getTimeStamp());
		assertFalse(tt.hasNext());
		assertNull(tt.nextElement());
	}
}
