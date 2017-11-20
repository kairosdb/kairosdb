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
package org.kairosdb.datastore.cassandra;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
import org.kairosdb.core.datapoints.LegacyDataPointFactory;
import org.kairosdb.util.StringPool;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.SortedMap;

public class DataPointsRowKeySerializer extends AbstractSerializer<DataPointsRowKey>
{
	public static final Charset UTF8 = Charset.forName("UTF-8");

	private StringPool m_stringPool;

	public DataPointsRowKeySerializer()
	{
		this(false);
	}

	public DataPointsRowKeySerializer(boolean poolStrings)
	{
		if (poolStrings)
			m_stringPool = new StringPool();
	}

	/**
	 If we are pooling strings the string from the pool will be returned.
	 @param str string
	 @return returns the string or what's in the string pool if using a string pool
	 */
	private String getString(String str)
	{
		if (m_stringPool != null)
			return (m_stringPool.getString(str));
		else
			return (str);
	}

	@Override
	public ByteBuffer toByteBuffer(final DataPointsRowKey dataPointsRowKey)
	{
		ByteBuffer buffer = dataPointsRowKey.getSerializedBuffer();
		if (buffer != null)
		{
			buffer = buffer.duplicate();
		}
		else
		{
			int size = 8; //size of timestamp
			byte[] metricName = dataPointsRowKey.getMetricName().getBytes(UTF8);
			size += metricName.length;
			size++; //Add one for null at end of string

			//if the data type is null then we are creating a row key for the old
			//format - this is for delete operations
			byte[] dataType = null;
			String dataTypeStr = dataPointsRowKey.getDataType();
			if (!dataTypeStr.equals(LegacyDataPointFactory.DATASTORE_TYPE))
			{
				dataType = dataPointsRowKey.getDataType().getBytes(UTF8);
				size += dataType.length;
				size += 2; //for null marker and datatype size
			}

			byte[] tagString = generateTagString(dataPointsRowKey.getTags()).getBytes(UTF8);
			size += tagString.length;

			buffer = ByteBuffer.allocate(size);
			buffer.put(metricName); //Metric name is put in this way for sorting purposes
			buffer.put((byte) 0x0);
			buffer.putLong(dataPointsRowKey.getTimestamp());
			if (dataType != null)
			{
				if (dataPointsRowKey.isEndSearchKey())
					buffer.put((byte)0xFF); //Only used for serialization of end search keys
				else
					buffer.put((byte)0x0); //Marks the beginning of datatype
				buffer.put((byte) dataType.length);
				buffer.put(dataType);
			}
			buffer.put(tagString);

			buffer.flip();
			buffer = buffer.duplicate();
		}

		return buffer;
	}


	private String generateTagString(SortedMap<String, String> tags)
	{
		StringBuilder sb = new StringBuilder();
		for (String key : tags.keySet())
		{
			sb.append(key).append("=");
			sb.append(tags.get(key)).append(":");
		}

		return (sb.toString());
	}

	private void extractTags(DataPointsRowKey rowKey, String tagString)
	{
		int mark = 0;
		int position = 0;
		String tag = null;
		String value;

		for (position = 0; position < tagString.length(); position ++)
		{
			if (tag == null)
			{
				if (tagString.charAt(position) == '=')
				{
					tag = tagString.substring(mark, position);
					mark = position +1;
				}
			}
			else
			{
				if (tagString.charAt(position) == ':')
				{
					value = tagString.substring(mark, position);
					mark = position +1;

					rowKey.addTag(getString(tag), getString(value));
					tag = null;
				}
			}
		}
	}


	@Override
	public DataPointsRowKey fromByteBuffer(ByteBuffer byteBuffer)
	{
		int start = byteBuffer.position();
		byteBuffer.mark();
		//Find null
		while (byteBuffer.get() != 0x0);

		int nameSize = (byteBuffer.position() - start) -1;
		byteBuffer.reset();

		byte[] metricName = new byte[nameSize];
		byteBuffer.get(metricName);
		byteBuffer.get(); //Skip the null

		long timestamp = byteBuffer.getLong();

		//Check for datatype marker which ia a null
		byteBuffer.mark();
		//default to legacy type
		String dataType = LegacyDataPointFactory.DATASTORE_TYPE;
		if (byteBuffer.get() == 0x0)
		{
			int dtSize = byteBuffer.get();
			byte[] dataTypeBytes = new byte[dtSize];
			byteBuffer.get(dataTypeBytes);
			dataType = new String(dataTypeBytes, UTF8);
		}
		else
		{
			byteBuffer.reset();
		}

		DataPointsRowKey rowKey = new DataPointsRowKey(getString(new String(metricName, UTF8)),
				timestamp, dataType);

		byte[] tagString = new byte[byteBuffer.remaining()];
		byteBuffer.get(tagString);

		String tags = new String(tagString, UTF8);

		extractTags(rowKey, tags);

		return rowKey;
	}
}
