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

import org.kairosdb.core.datapoints.DataPointHelper;
import org.kairosdb.datastore.cassandra.ValueSerializer;
import org.kairosdb.util.Util;

import java.nio.ByteBuffer;

/**
 * Represents a single data point.
 * <p>
 * Implementations of this interface aren't expected to be synchronized.
 */
public class LegacyDataPoint extends DataPointHelper implements NumericDataPoint
{
	private boolean m_isInteger;
	private long m_longValue;
	private double m_doubleValue;
	private ByteBuffer m_byteBuffer;

	public LegacyDataPoint(long timestamp, long value)
	{
		super(timestamp);
		m_isInteger = true;
		m_longValue = value;
	}

	public LegacyDataPoint(long timestamp, double value)
	{
		super(timestamp);
		m_isInteger = false;
		m_doubleValue = value;
	}


	@Override
	public ByteBuffer toByteBuffer()
	{
		if (m_byteBuffer != null)
		{
			if (m_isInteger)
				m_byteBuffer = ValueSerializer.toByteBuffer(m_longValue);
			else
				m_byteBuffer = ValueSerializer.toByteBuffer((float)m_doubleValue);
		}

		return (m_byteBuffer);  //To change body of implemented methods use File | Settings | File Templates.
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
	public String getApiDataType()
	{
		if (m_isInteger)
			return (API_LONG);
		else
			return (API_DOUBLE);
	}

	/**
	 Legacy data points are no longer written to the datastore
	 @return Returns null.
	 */
	@Override
	public String getDataStoreDataType()
	{
		return null;  //To change body of implemented methods use File | Settings | File Templates.
	}
}
