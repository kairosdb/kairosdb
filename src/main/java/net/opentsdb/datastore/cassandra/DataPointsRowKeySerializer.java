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
package net.opentsdb.datastore.cassandra;

import me.prettyprint.cassandra.serializers.AbstractSerializer;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.SortedMap;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 1/11/13
 Time: 10:07 PM
 To change this template use File | Settings | File Templates.
 */
public class DataPointsRowKeySerializer extends AbstractSerializer<DataPointsRowKey>
{
	public static final Charset UTF8 = Charset.forName("UTF-8");

	@Override
	public ByteBuffer toByteBuffer(DataPointsRowKey dataPointsRowKey)
	{
		int size = 8; //size of timestamp
		byte[] metricName = dataPointsRowKey.getMetricName().getBytes(UTF8);
		size += metricName.length;
		size++; //Add one for null at end of string
		byte[] tagString = generateTagString(dataPointsRowKey.getTags()).getBytes(UTF8);
		size += tagString.length;

		ByteBuffer buffer = ByteBuffer.allocate(size);
		buffer.put(metricName);
		buffer.put((byte)0x0);
		buffer.putLong(dataPointsRowKey.getTimestamp());
		buffer.put(tagString);

		buffer.flip();

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

					rowKey.addTag(tag, value);
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

		DataPointsRowKey rowKey = new DataPointsRowKey(new String(metricName, UTF8),
				timestamp);

		byte[] tagString = new byte[byteBuffer.remaining()];
		byteBuffer.get(tagString);

		String tags = new String(tagString, UTF8);

		extractTags(rowKey, tags);

		return rowKey;
	}
}
