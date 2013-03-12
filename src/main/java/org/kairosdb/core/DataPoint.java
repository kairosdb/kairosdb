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
