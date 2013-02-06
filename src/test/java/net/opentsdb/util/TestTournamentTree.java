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


import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestTournamentTree
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
		TournamentTree<RowData> tt = new TournamentTree<RowData>(new RowDataComparator());

		List<RowData> list1 = new ArrayList<RowData>();
		list1.add(new RowData(1, 0));
		list1.add(new RowData(2, 0));
		list1.add(new RowData(3, 0));
		list1.add(new RowData(4, 0));

		List<RowData> list2 = new ArrayList<RowData>();
		list2.add(new RowData(5, 0));
		list2.add(new RowData(6, 0));
		list2.add(new RowData(7, 0));
		list2.add(new RowData(8, 0));

		List<RowData> list3 = new ArrayList<RowData>();
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
		TournamentTree<RowData> tt = new TournamentTree<RowData>(new RowDataComparator());

		List<RowData> list1 = new ArrayList<RowData>();
		list1.add(new RowData(1, 0));
		list1.add(new RowData(3, 0));
		list1.add(new RowData(5, 0));
		list1.add(new RowData(7, 0));

		List<RowData> list2 = new ArrayList<RowData>();
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
		TournamentTree<RowData> tt = new TournamentTree<RowData>(new RowDataComparator());

		List<RowData> list1 = new ArrayList<RowData>();
		list1.add(new RowData(1, 0));
		list1.add(new RowData(3, 0));
		list1.add(new RowData(5, 0));
		list1.add(new RowData(7, 0));

		List<RowData> list2 = new ArrayList<RowData>();
		list2.add(new RowData(1, 0));
		list2.add(new RowData(2, 0));
		list2.add(new RowData(3, 0));
		list2.add(new RowData(4, 0));

		List<RowData> list3 = new ArrayList<RowData>();
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
