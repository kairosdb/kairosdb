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
package org.kairosdb.core;

import org.kairosdb.util.Util;

/**
 * Represents a single data point.
 * <p>
 * Implementations of this interface aren't expected to be synchronized.
 */
public class DataPoint implements Comparable<DataPoint>
{
	private long m_timestamp;
	private boolean m_isInteger;
	private long m_longValue;
	private double m_doubleValue;

	public DataPoint(long timestamp, long value)
	{
		m_isInteger = true;
		m_timestamp = timestamp;
		m_longValue = value;
	}

	public DataPoint(long timestamp, double value)
	{
		m_isInteger = false;
		m_timestamp = timestamp;
		m_doubleValue = value;
	}

	/**
	 Get the timestamp for this data point in milliseconds
	 @return timestamp
	 */
	public long getTimestamp()
	{
		return m_timestamp;
	}

	public boolean isInteger()
	{
		return m_isInteger;
	}

	public double getDoubleValue()
	{
		return m_isInteger ? (double) m_longValue : m_doubleValue;
	}

	public long getLongValue()
	{
		return m_isInteger ? m_longValue : (long)m_doubleValue;
	}


	@Override
	public String toString()
	{
		return "DataPoint{" +
				"m_timestamp=" + m_timestamp +
				", m_isInteger=" + m_isInteger +
				", m_longValue=" + m_longValue +
				", m_doubleValue=" + m_doubleValue +
				'}';
	}

	@Override
	public int compareTo(DataPoint o)
	{
		long ret = getTimestamp() - o.getTimestamp();

		if (ret == 0L)
		{
			if (m_isInteger)
				return (Util.compareLong(m_longValue, o.getLongValue()));
			else
				return (Double.compare(m_doubleValue, o.getDoubleValue()));
		}
		else
			return (ret < 0L ? -1 : 1);
	}
}
