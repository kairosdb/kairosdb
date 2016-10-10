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
package org.kairosdb.core.groupby;

import com.google.common.collect.HashMultimap;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.datapoints.DataPointFactory;
import org.kairosdb.core.datastore.DataPointGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.kairosdb.util.Util.packLong;
import static org.kairosdb.util.Util.unpackLong;

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

	private File m_groupCacheFile;
	private DataOutputStream m_dataOutputStream;
	private List<GroupByResult> groupByResults;
	private String name;
	private HashMultimap<String, String> tags = HashMultimap.create();
	private int m_dataPointCount; //Number of datapoints written to file

	private final KairosDataPointFactory dataPointFactory;
	private final Map<String, Integer> storageTypeIdMap;
	private final List<DataPointFactory> dataPointFactories;

	private Group(File file, DataPointGroup dataPointGroup, List<GroupByResult> groupByResults,
			KairosDataPointFactory dataPointFactory) throws FileNotFoundException
	{
		checkNotNull(file);
		checkNotNull(groupByResults);
		checkNotNull(dataPointGroup);

		this.dataPointFactory = dataPointFactory;
		storageTypeIdMap = new HashMap<String, Integer>();
		dataPointFactories = new ArrayList<DataPointFactory>();

		m_groupCacheFile = file;

		m_dataOutputStream = new DataOutputStream(new BufferedOutputStream(
				new FileOutputStream(m_groupCacheFile)));

		this.groupByResults = groupByResults;
		this.name = dataPointGroup.getName();

		addTags(dataPointGroup);
	}

	public static Group createGroup(DataPointGroup dataPointGroup, List<Integer> groupIds,
			List<GroupByResult> groupByResults, KairosDataPointFactory dataPointFactory) throws IOException
	{
		checkNotNull(dataPointGroup);
		checkNotNull(groupIds);
		checkNotNull(groupByResults);

		return new Group(getFile(groupIds), dataPointGroup, groupByResults, dataPointFactory);
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

	private int getStorageTypeId(String storageType)
	{
		Integer id = storageTypeIdMap.get(storageType);
		if (id == null)
		{
			id = dataPointFactories.size();
			storageTypeIdMap.put(storageType, dataPointFactories.size());
			dataPointFactories.add(dataPointFactory.getFactoryForDataStoreType(storageType));
		}

		return id;
	}

	public void addDataPoint(DataPoint dataPoint) throws IOException
	{
		m_dataPointCount ++;
		packLong(dataPoint.getTimestamp(), m_dataOutputStream);
		int id = getStorageTypeId(dataPoint.getDataStoreDataType());
		packLong(id, m_dataOutputStream);
		dataPoint.writeValueToBuffer(m_dataOutputStream);
	}

	public void addGroupByResults(List<GroupByResult> results)
	{
		groupByResults.addAll(checkNotNull(results));
	}

	public DataPointGroup getDataPointGroup() throws IOException
	{
		m_dataOutputStream.flush();
		m_dataOutputStream.close();

		return (new CachedDataPointGroup());
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
		private int m_readCount = 0; //number of datapoints read from file
		private DataInputStream m_dataInputStream;

		private CachedDataPointGroup() throws IOException
		{
			m_dataInputStream = new DataInputStream(new BufferedInputStream(
					new FileInputStream(m_groupCacheFile)));
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
				m_dataInputStream.close();
				boolean fileDeleted = m_groupCacheFile.delete();

				if (!fileDeleted)
					logger.error("Could not delete group file: " + m_groupCacheFile.getAbsolutePath());

			}
			catch (IOException e)
			{
				logger.error("Failed to close group file: " + m_groupCacheFile.getAbsolutePath());
			}
		}

		@Override
		public boolean hasNext()
		{
			return (m_readCount < m_dataPointCount);
		}

		@Override
		public DataPoint next()
		{
			DataPoint dataPoint;

			if (m_readCount == m_dataPointCount)
				return null;

			try
			{
				long timestamp = unpackLong(m_dataInputStream);
				int typeId = (int)unpackLong(m_dataInputStream);

				dataPoint = dataPointFactories.get(typeId).getDataPoint(timestamp, m_dataInputStream);
				m_readCount ++;

			}
			catch (IOException e)
			{
				// todo do I need to throw the exception?
				logger.error("Error reading from group file: " + m_groupCacheFile.getAbsolutePath(), e);
				return null;
			}

			return (dataPoint);
		}

		@Override
		public void remove()
		{
			throw new UnsupportedOperationException();
		}
	}
}