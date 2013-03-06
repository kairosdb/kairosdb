// KairosDB2
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

package org.kairosdb.datastore.cassandra;

public class LongOrDouble
{
	private boolean m_isLong;
	private long m_longValue;
	private double m_doubleValue;

	public LongOrDouble(long longValue)
	{
		m_isLong = true;
		m_longValue = longValue;
	}

	public LongOrDouble(double doubleValue)
	{
		m_isLong = false;
		m_doubleValue = doubleValue;
	}

	public boolean isLong()
	{
		return m_isLong;
	}

	public long getLongValue()
	{
		return m_longValue;
	}

	public double getDoubleValue()
	{
		return m_doubleValue;
	}
}
