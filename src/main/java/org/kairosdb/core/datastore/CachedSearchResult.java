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

package org.kairosdb.core.datastore;

import org.kairosdb.core.DataPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class CachedSearchResult
{
	public static final Logger logger = LoggerFactory.getLogger(CachedSearchResult.class);

	public static final int DATA_POINT_SIZE = 8 + 1 + 8; //timestamp + type flag + value
	public static final int READ_BUFFER_SIZE = 60; //The number of datapoints to read into each buffer we could potentially have a lot of these so we keep them smaller
	public static final int WRITE_BUFFER_SIZE = 500;

	public static final byte LONG_FLAG = 0x1;
	public static final byte DOUBLE_FLAG = 0x2;

	private String m_metricName;
	private List<FilePositionMarker> m_dataPointSets;
	private ByteBuffer m_writeBuffer;
	private FileChannel m_dataFileChannel;
	private File m_indexFile;
	private AtomicInteger m_closeCounter = new AtomicInteger();

	private static File getIndexFile(String baseFileName)
	{
		String indexFileName = baseFileName + ".index";

		return (new File(indexFileName));
	}

	private static File getDataFile(String baseFileName)
	{
		String dataFileName = baseFileName+".data";

		return (new File(dataFileName));
	}

	private CachedSearchResult(String metricName, File dataFile, File indexFile)
			throws FileNotFoundException
	{
		m_metricName = metricName;
		m_writeBuffer = ByteBuffer.allocate(DATA_POINT_SIZE * WRITE_BUFFER_SIZE);
		m_writeBuffer.clear();
		m_indexFile = indexFile;
		m_dataPointSets = new ArrayList<FilePositionMarker>();

		RandomAccessFile rFile = new RandomAccessFile(dataFile, "rw");

		m_dataFileChannel = rFile.getChannel();
	}


	/**
	 Reads the index file into memory
	 */
	private void loadIndex()
	{
		//TODO:
	}

	private void saveIndex()
	{
		//TODO;
	}

	private void clearDataFile() throws IOException
	{
		m_dataFileChannel.truncate(0);
	}

	public static CachedSearchResult createCachedSearchResult(String metricName,
			String baseFileName)
			throws IOException
	{
		File dataFile = getDataFile(baseFileName);
		File indexFile = getIndexFile(baseFileName);

		CachedSearchResult ret = new CachedSearchResult(metricName, dataFile, indexFile);

		ret.clearDataFile();

		return (ret);
	}

	/**

	 @param baseFileName
	 @param cacheTime The number of milliseconds to still open the file
	 @return The CachedSearchResult if the file exists or null if it doesn't
	 */
	public static CachedSearchResult openCachedSearchResult(String metricName,
			String baseFileName, int cacheTime) throws IOException
	{
		CachedSearchResult ret = null;
		File dataFile = getDataFile(baseFileName);
		long now = System.currentTimeMillis();

		if (dataFile.exists() && ((now - dataFile.lastModified()) < cacheTime))
		{
			File indexFile = getIndexFile(baseFileName);

			ret = new CachedSearchResult(metricName, dataFile, indexFile);
			ret.loadIndex();
		}

		return (ret);
	}

	/**
	 Call when finished adding datapoints to the cache file
	 */
	public void endDataPoints() throws IOException
	{
		flushWriteBuffer();

		long curPosition = m_dataFileChannel.position();
		if (m_dataPointSets.size() != 0)
			((FilePositionMarker)m_dataPointSets.get(m_dataPointSets.size() -1)).setEndPosition(curPosition);

		saveIndex();
	}

	/**
	 Closes the underling file handle
	 @throws IOException
	 */
	public void close()
	{
		try
		{
			m_dataFileChannel.close();
		}
		catch (IOException e)
		{
			logger.error("Failure closing cache file", e);
		}
	}

	protected void decrementClose()
	{
		if (m_closeCounter.decrementAndGet() == 0)
			close();
	}


	/**
	 A new set of datapoints to write to the file.  This causes the start position
	 of the set to be saved.  All inserted datapoints after this call are
	 expected to be in ascending time order and have the same tags.
	 */
	public void startDataPointSet(Map<String, String> tags) throws IOException
	{
		endDataPoints();

		long curPosition = m_dataFileChannel.position();
		m_dataPointSets.add(new FilePositionMarker(curPosition, tags));
	}

	private void flushWriteBuffer() throws IOException
	{
		if (m_writeBuffer.position() != 0)
		{
			m_writeBuffer.flip();

			while (m_writeBuffer.hasRemaining())
				m_dataFileChannel.write(m_writeBuffer);

			m_writeBuffer.clear();
		}
	}

	public void addDataPoint(long timestamp, long value) throws IOException
	{
		if (!m_writeBuffer.hasRemaining())
		{
			flushWriteBuffer();
		}
		m_writeBuffer.putLong(timestamp);
		m_writeBuffer.put(LONG_FLAG);
		m_writeBuffer.putLong(value);
	}

	public void addDataPoint(long timestamp, double value) throws IOException
	{
		if (!m_writeBuffer.hasRemaining())
		{
			flushWriteBuffer();
		}
		m_writeBuffer.putLong(timestamp);
		m_writeBuffer.put(DOUBLE_FLAG);
		m_writeBuffer.putDouble(value);
	}

	public List<DataPointRow> getRows()
	{
		List<DataPointRow> ret = new ArrayList<DataPointRow>();
		for (FilePositionMarker dpSet : m_dataPointSets)
		{
			ret.add(dpSet.iterator());
			m_closeCounter.incrementAndGet();
		}

		return (ret);
	}

	//===========================================================================
	private class FilePositionMarker implements Iterable<DataPoint>
	{
		private long m_startPosition;
		private long m_endPosition;
		private Map<String, String> m_tags;

		public FilePositionMarker(long startPosition, Map<String, String> tags)
		{
			m_startPosition = startPosition;
			m_tags = tags;
		}

		public void setEndPosition(long endPosition)
		{
			m_endPosition = endPosition;
		}

		public Map<String, String> getTags()
		{
			return m_tags;
		}

		@Override
		public CachedDataPointRow iterator()
		{
			return (new CachedDataPointRow(m_tags, m_startPosition, m_endPosition));
		}
	}

	//===========================================================================
	private class CachedDataPointRow implements DataPointRow
	{
		private long m_currentPosition;
		private long m_endPostition;
		private ByteBuffer m_readBuffer;
		private Map<String, String> m_tags;

		public CachedDataPointRow(Map<String, String> tags,
				long startPosition, long endPostition)
		{
			m_currentPosition = startPosition;
			m_endPostition = endPostition;
			m_readBuffer = ByteBuffer.allocate(DATA_POINT_SIZE * READ_BUFFER_SIZE);
			m_readBuffer.clear();
			m_readBuffer.limit(0);
			m_tags = tags;
		}

		private void readMorePoints() throws IOException
		{
			m_readBuffer.clear();

			if ((m_endPostition - m_currentPosition) < m_readBuffer.limit())
				m_readBuffer.limit((int)(m_endPostition - m_currentPosition));

			int sizeRead = m_dataFileChannel.read(m_readBuffer, m_currentPosition);
			if (sizeRead == -1)
				throw new IOException("Prematurely reached the end of the file");

			m_currentPosition += sizeRead;
			m_readBuffer.flip();
		}

		@Override
		public boolean hasNext()
		{
			return (m_readBuffer.hasRemaining() || m_currentPosition < m_endPostition);
		}

		@Override
		public DataPoint next()
		{
			DataPoint ret = null;

			try
			{
				if (!m_readBuffer.hasRemaining())
					readMorePoints();

				if (!m_readBuffer.hasRemaining())
					return (null);

				long timestamp = m_readBuffer.getLong();
				byte flag = m_readBuffer.get();
				if (flag == LONG_FLAG)
					ret = new DataPoint(timestamp, m_readBuffer.getLong());
				else
					ret = new DataPoint(timestamp, m_readBuffer.getDouble());

			}
			catch (IOException ioe)
			{
				logger.error("Error reading next data point.", ioe);
			}

			return (ret);
		}

		@Override
		public void remove()
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public String getName()
		{
			return (m_metricName);
		}

		@Override
		public Set<String> getTagNames()
		{
			return (m_tags.keySet());
		}

		@Override
		public String getTagValue(String tag)
		{
			return (m_tags.get(tag));
		}

		@Override
		public void close()
		{
			decrementClose();
		}
	}
}
