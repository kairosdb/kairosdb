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
package org.kairosdb.core.groupby;

import com.google.common.collect.HashMultimap;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datastore.DataPointGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 *  A grouping of data points. The group is written to disk.
 */
public class Group
{
	public static final Logger logger = LoggerFactory.getLogger(Group.class);

	public static final int DATA_POINT_SIZE = 8 + 1 + 8; //timestamp + type flag + value
	public static final int READ_BUFFER_SIZE = 60; //The number of data points to read into each buffer we could potentially have a lot of these so we keep them smaller
	public static final int WRITE_BUFFER_SIZE = 500;

	public static final byte LONG_FLAG = 0x1;
	public static final byte DOUBLE_FLAG = 0x2;

	private ByteBuffer writeBuffer;
	private FileChannel dataFileChannel;
	private List<GroupByResult> groupByResults;
	private String name;
	private HashMultimap<String, String> tags = HashMultimap.create();
	private File file;

	private Group(File file, DataPointGroup dataPointGroup, List<GroupByResult> groupByResults) throws FileNotFoundException
	{
		checkNotNull(file);
		checkNotNull(groupByResults);
		checkNotNull(dataPointGroup);

		writeBuffer = ByteBuffer.allocate(DATA_POINT_SIZE * WRITE_BUFFER_SIZE);
		writeBuffer.clear();

		RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
		dataFileChannel = randomAccessFile.getChannel();

		this.groupByResults = groupByResults;
		this.name = dataPointGroup.getName();
		this.file = file;

		addTags(dataPointGroup);
	}

	public static Group createGroup(DataPointGroup dataPointGroup, List<Integer> groupIds, List<GroupByResult> groupByResults) throws IOException
	{
		checkNotNull(dataPointGroup);
		checkNotNull(groupIds);
		checkNotNull(groupByResults);

		return new Group(getFile(groupIds), dataPointGroup, groupByResults);
	}

	private static File getFile(List<Integer> groupIds) throws IOException
	{
		StringBuilder builder = new StringBuilder();
		for (Integer groupId : groupIds)
		{
			builder.append(groupId);
		}

		return File.createTempFile("grouper-" + builder.toString(), ".cache");
	}

	public void addDataPoint(DataPoint dataPoint) throws IOException
	{
		if (!writeBuffer.hasRemaining())
		{
			flushWriteBuffer();
		}

		writeBuffer.putLong(dataPoint.getTimestamp());
		if (dataPoint.isInteger())
		{
			writeBuffer.put(LONG_FLAG);
			writeBuffer.putLong(dataPoint.getLongValue());
		}
		else
		{
			writeBuffer.put(DOUBLE_FLAG);
			writeBuffer.putDouble(dataPoint.getDoubleValue());
		}
	}

	private void flushWriteBuffer() throws IOException
	{
		if (writeBuffer.position() != 0)
		{
			writeBuffer.flip();

			while (writeBuffer.hasRemaining())
				dataFileChannel.write(writeBuffer);

			writeBuffer.clear();
		}
	}

	public void addGroupByResults(List<GroupByResult> results)
	{
		groupByResults.addAll(checkNotNull(results));
	}

	public DataPointGroup getDataPointGroup() throws IOException
	{
		flushWriteBuffer();

		return (new CachedDataPointGroup(file, dataFileChannel, name, tags, groupByResults));
	}

	/**
	 * Adds all tags from the data point group.
	 * @param dataPointGroup data point group
	 */
	public void addTags(DataPointGroup dataPointGroup)
	{
		for (String tagName : dataPointGroup.getTagNames())
		{
			tags.putAll(tagName, dataPointGroup.getTagValues(tagName));
		}
	}

	private class CachedDataPointGroup implements DataPointGroup
	{
		private List<GroupByResult> groupByResults;
		private ByteBuffer readBuffer;
		private FileChannel fileChannel;
		private long endPosition;
		private long currentPosition;
		private String name;
		private HashMultimap<String, String> tags;
		private File file;

		private CachedDataPointGroup(File file,
		                             FileChannel fileChannel,
		                             String name,
		                             HashMultimap<String, String> tags,
		                             List<GroupByResult> groupByResults) throws IOException
		{
			this.groupByResults = groupByResults;
			this.fileChannel = fileChannel;
			this.name = name;
			this.tags = tags;
			this.file = file;

			endPosition = fileChannel.position();
			fileChannel.position(0);

			readBuffer = ByteBuffer.allocate(DATA_POINT_SIZE * READ_BUFFER_SIZE);
			readBuffer.clear();
			readBuffer.limit(0);
		}

		@Override
		public String getName()
		{
			return name;
		}

		@Override
		public Set<String> getTagNames()
		{
			return tags.keySet();
		}

		@Override
		public Set<String> getTagValues(String tag)
		{
			return tags.get(tag);
		}

		@Override
		public List<GroupByResult> getGroupByResult()
		{
			return groupByResults;
		}

		@Override
		public void close()
		{
			try
			{
				fileChannel.close();
				boolean fileDeleted = file.delete();

				if (!fileDeleted)
					logger.error("Could not delete group file: " + file.getAbsolutePath());
			}
			catch (IOException e)
			{
				logger.error("Failed to close group file: " + file.getAbsolutePath());
			}
		}

		@Override
		public boolean hasNext()
		{
			return (readBuffer.hasRemaining() || currentPosition < endPosition);
		}

		@Override
		public DataPoint next()
		{
			DataPoint dataPoint;

			try
			{
				if (!readBuffer.hasRemaining())
					readMorePoints();

				if (!readBuffer.hasRemaining())
					return (null);

				long timestamp = readBuffer.getLong();
				byte flag = readBuffer.get();
				if (flag == LONG_FLAG)
					dataPoint = new DataPoint(timestamp, readBuffer.getLong());
				else
					dataPoint = new DataPoint(timestamp, readBuffer.getDouble());
			}
			catch (IOException e)
			{
				// todo do I need to throw the exception?
				logger.error("Error reading from group file: " + file.getAbsolutePath(), e);
				return null;
			}

			return (dataPoint);
		}

		@Override
		public void remove()
		{
			throw new UnsupportedOperationException();
		}

		private void readMorePoints() throws IOException
		{
			readBuffer.clear();

			if ((endPosition - currentPosition) < readBuffer.limit())
				readBuffer.limit((int)(endPosition - currentPosition));

			int sizeRead = fileChannel.read(readBuffer, currentPosition);
			if (sizeRead == -1)
				throw new IOException("Prematurely reached the end of the file");

			currentPosition += sizeRead;
			readBuffer.flip();
		}
	}
}