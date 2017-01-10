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
package org.kairosdb.datastore.cassandra;

import java.nio.ByteBuffer;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;

public class DataPointsRowKey
{
	private final String m_metricName;
	private final long m_timestamp;
	private final String m_dataType;
	private final SortedMap<String, String> m_tags;
	private boolean m_endSearchKey; //Only used for end slice operations.  Serialization
	//adds a 0xFF after the timestamp to make sure we get all data for that timestamp.

	private ByteBuffer m_serializedBuffer;

	public DataPointsRowKey(String metricName, long timestamp, String dataType)
	{
		this(metricName, timestamp, dataType, new TreeMap<String, String>());
	}

	public DataPointsRowKey(String metricName, long timestamp, String datatype,
			SortedMap<String, String> tags)
	{
		m_metricName = checkNotNullOrEmpty(metricName);
		m_timestamp = timestamp;
		m_dataType = checkNotNull(datatype);
		m_tags = tags;

	}

	public void addTag(String name, String value)
	{
		m_tags.put(name, value);
	}

	public String getMetricName()
	{
		return m_metricName;
	}

	public SortedMap<String, String> getTags()
	{
		return m_tags;
	}

	public long getTimestamp()
	{
		return m_timestamp;
	}

	public boolean isEndSearchKey()
	{
		return m_endSearchKey;
	}

	public void setEndSearchKey(boolean endSearchKey)
	{
		m_endSearchKey = endSearchKey;
	}

	/**
	 If this returns "" (empty string)` then it is the old row key format and the data type
	 is determined by the timestamp bit in the column.
	 @return
	 */
	public String getDataType()
	{
		return m_dataType;
	}

	@Override
	public boolean equals(Object o)
	{
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		DataPointsRowKey that = (DataPointsRowKey) o;

		if (m_timestamp != that.m_timestamp) return false;
		if (m_dataType != null ? !m_dataType.equals(that.m_dataType) : that.m_dataType != null)
			return false;
		if (!m_metricName.equals(that.m_metricName)) return false;
		if (!m_tags.equals(that.m_tags)) return false;

		return true;
	}

	@Override
	public int hashCode()
	{
		int result = m_metricName.hashCode();
		result = 31 * result + (int) (m_timestamp ^ (m_timestamp >>> 32));
		result = 31 * result + (m_dataType != null ? m_dataType.hashCode() : 0);
		result = 31 * result + m_tags.hashCode();
		return result;
	}

	@Override
	public String toString()
	{
		return "DataPointsRowKey{" +
				"m_metricName='" + m_metricName + '\'' +
				", m_timestamp=" + m_timestamp +
				", m_dataType='" + m_dataType + '\'' +
				", m_tags=" + m_tags +
				'}';
	}

	public ByteBuffer getSerializedBuffer()
	{
		return m_serializedBuffer;
	}

	public void setSerializedBuffer(ByteBuffer serializedBuffer)
	{
		m_serializedBuffer = serializedBuffer;
	}
}
