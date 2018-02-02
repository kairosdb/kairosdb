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

package org.kairosdb.core.datastore;

import org.kairosdb.core.DataPoint;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.util.BufferedDataInputStream;
import org.kairosdb.util.BufferedDataOutputStream;
import org.kairosdb.util.MemoryMonitor;
import org.kairosdb.util.StringPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.Externalizable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CachedSearchResult implements SearchResult
{
	public static final Logger logger = LoggerFactory.getLogger(CachedSearchResult.class);

	public static final int WRITE_BUFFER_SIZE = 500;

	public static final byte LONG_FLAG = 0x1;
	public static final byte DOUBLE_FLAG = 0x2;

	private final String m_metricName;
	private final List<FilePositionMarker> m_dataPointSets;
	private final MemoryMonitor m_memoryMonitor;
	private FilePositionMarker m_currentFilePositionMarker;
	private final File m_dataFile;
	private RandomAccessFile m_randomAccessFile;
	private BufferedDataOutputStream m_dataOutputStream;

	private final File m_indexFile;
	private final AtomicInteger m_closeCounter = new AtomicInteger();
	private boolean m_readFromCache = false;
	private final KairosDataPointFactory m_dataPointFactory;
	private final StringPool m_stringPool;
	private int m_maxReadBufferSize = 8192;  //Default value in BufferedInputStream
	private boolean m_keepCacheFiles;
	private final ReentrantReadWriteLock m_lock = new ReentrantReadWriteLock();


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

	private CachedSearchResult(String metricName, File dataFile, File indexFile,
			KairosDataPointFactory datatPointFactory, boolean keepCacheFiles)
			throws FileNotFoundException
	{
		m_metricName = metricName;
		m_indexFile = indexFile;
		m_dataPointSets = new ArrayList<FilePositionMarker>();
		m_dataFile = dataFile;
		m_dataPointFactory = datatPointFactory;
		m_stringPool = new StringPool();
		m_keepCacheFiles = keepCacheFiles;
		m_memoryMonitor = new MemoryMonitor(1000);
	}

	private void openCacheFile() throws FileNotFoundException
	{
		//Cache cleanup could have removed the folders
		m_dataFile.getParentFile().mkdirs();
		m_randomAccessFile = new RandomAccessFile(m_dataFile, "rw");
		m_dataOutputStream = BufferedDataOutputStream.create(m_randomAccessFile, 0L);
	}

	private void calculateMaxReadBufferSize()
	{
		//Reduce the max buffer size when we have a lot of rows to conserve memory
		if (m_dataPointSets.size() > 100000)
			m_maxReadBufferSize = 1024;
		else if (m_dataPointSets.size() > 75000)
			m_maxReadBufferSize = 1024 * 2;
		else if (m_dataPointSets.size() > 50000)
			m_maxReadBufferSize = 1024 * 4;
	}


	/**
	 Reads the index file into memory
	 */
	private void loadIndex() throws IOException, ClassNotFoundException
	{
		ObjectInputStream in = new ObjectInputStream(new FileInputStream(m_indexFile));
		int size = in.readInt();
		for (int I = 0; I < size; I++)
		{
			//open the cache file only if there will be data point groups returned
			if (m_randomAccessFile == null)
				openCacheFile();

			FilePositionMarker marker = new FilePositionMarker();
			marker.readExternal(in);
			m_dataPointSets.add(marker);
		}


		m_readFromCache = true;
		in.close();

		calculateMaxReadBufferSize();
	}

	private void saveIndex() throws IOException
	{
		if (m_readFromCache)
			return; //No need to save if we read it from the file

		ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(m_indexFile));

		//todo: write out a type lookup table

		out.writeInt(m_dataPointSets.size());
		for (FilePositionMarker marker : m_dataPointSets)
		{
			marker.writeExternal(out);
		}

		out.flush();
		out.close();
	}


	public static CachedSearchResult createCachedSearchResult(String metricName,
			String baseFileName, KairosDataPointFactory dataPointFactory,
			boolean keepCacheFiles)
			throws IOException
	{
		File dataFile = getDataFile(baseFileName);
		File indexFile = getIndexFile(baseFileName);

		//Just in case the file are there.
		dataFile.delete();
		indexFile.delete();

		CachedSearchResult ret = new CachedSearchResult(metricName, dataFile,
				indexFile, dataPointFactory, keepCacheFiles);

		return (ret);
	}

	/**

	 @param baseFileName base name of file
	 @param cacheTime The number of seconds to still open the file
	 @return The CachedSearchResult if the file exists or null if it doesn't
	 */
	public static CachedSearchResult openCachedSearchResult(String metricName,
			String baseFileName, int cacheTime, KairosDataPointFactory dataPointFactory,
			boolean keepCacheFiles) throws IOException
	{
		CachedSearchResult ret = null;
		File dataFile = getDataFile(baseFileName);
		File indexFile = getIndexFile(baseFileName);
		long now = System.currentTimeMillis();

		if (dataFile.exists() && indexFile.exists() && ((now - dataFile.lastModified()) < ((long)cacheTime * 1000)))
		{

			ret = new CachedSearchResult(metricName, dataFile, indexFile, dataPointFactory, keepCacheFiles);
			try
			{
				ret.loadIndex();
			}
			catch (ClassNotFoundException e)
			{
				logger.error("Unable to load cache file", e);
				ret = null;
			}
		}

		return (ret);
	}



	/**
	 Closes the underling file handle
	 */
	private void close()
	{
		try
		{
			if (m_randomAccessFile != null)
				m_randomAccessFile.close();

			if (m_keepCacheFiles)
				saveIndex();
			else
				m_dataFile.delete();
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

	@Override
	public List<DataPointRow> getRows()
	{
		List<DataPointRow> ret = new ArrayList<DataPointRow>();
		MemoryMonitor mm = new MemoryMonitor(20);

		for (FilePositionMarker dpSet : m_dataPointSets)
		{
			ret.add(dpSet.iterator());
			m_closeCounter.incrementAndGet();
			mm.checkMemoryAndThrowException();
		}

		return (ret);
	}

	/**
	 A new set of datapoints to write to the file.  This causes the start position
	 of the set to be saved.  All inserted datapoints after this call are
	 expected to be in ascending time order and have the same tags.
	 */
	@Override
	public DataPointWriter startDataPointSet(String type, SortedMap<String, String> tags) throws IOException
	{
		return new CachedDatapointWriter(type, tags);
	}


	private class CachedDatapointWriter implements DataPointWriter
	{
		private final String m_dataType;
		private final Map<String, String> m_tags;
		private final List<DataPoint> m_dataPoints;

		public CachedDatapointWriter(String type, Map<String, String> tags)
		{
			m_dataType = type;
			m_tags = tags;
			m_dataPoints = new ArrayList<>();
		}

		@Override
		public void addDataPoint(DataPoint datapoint) throws IOException
		{
			m_dataPoints.add(datapoint);
			m_memoryMonitor.checkMemoryAndThrowException();
		}

		/**
		 Call when finished adding datapoints to the cache file
		 */
		@Override
		public void close() throws IOException
		{
			try
			{
				m_lock.writeLock().lock();

				if (m_randomAccessFile == null)
					openCacheFile();

				long curPosition = m_dataOutputStream.getPosition();
				m_currentFilePositionMarker = new FilePositionMarker(curPosition, m_tags, m_dataType);
				m_dataPointSets.add(m_currentFilePositionMarker);


				for (DataPoint dataPoint : m_dataPoints)
				{
					m_dataOutputStream.writeLong(dataPoint.getTimestamp());
					dataPoint.writeValueToBuffer(m_dataOutputStream);

					m_currentFilePositionMarker.incrementDataPointCount();
				}


				//flushWriteBuffer();
				m_dataOutputStream.flush();

				curPosition = m_dataOutputStream.getPosition();
				if (m_dataPointSets.size() != 0)
					m_dataPointSets.get(m_dataPointSets.size() - 1).setEndPosition(curPosition);

				calculateMaxReadBufferSize();
			}
			finally
			{
				m_lock.writeLock().unlock();
			}
		}

	}

	//===========================================================================
	private class FilePositionMarker implements Iterable<DataPoint>, Externalizable
	{
		private long m_startPosition;
		private long m_endPosition;
		private Map<String, String> m_tags;
		private String m_dataType;
		private int m_dataPointCount;


		public FilePositionMarker()
		{
			m_startPosition = 0L;
			m_endPosition = 0L;
			m_tags = new HashMap<String, String>();
			m_dataType = null;
			m_dataPointCount = 0;
		}

		public FilePositionMarker(long startPosition, Map<String, String> tags,
				String dataType)
		{
			m_startPosition = startPosition;
			m_tags = tags;
			m_dataType = dataType;
		}

		public void setEndPosition(long endPosition)
		{
			m_endPosition = endPosition;
		}

		public Map<String, String> getTags()
		{
			return m_tags;
		}

		public void incrementDataPointCount()
		{
			m_dataPointCount ++;
		}

		public int getDataPointCount()
		{
			return m_dataPointCount;
		}

		@Override
		public CachedDataPointRow iterator()
		{
			return (new CachedDataPointRow(m_tags, m_startPosition, m_endPosition,
					m_dataType, m_dataPointCount));
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException
		{
			out.writeLong(m_startPosition);
			out.writeLong(m_endPosition);
			out.writeInt(m_dataPointCount);
			out.writeObject(m_dataType);
			out.writeInt(m_tags.size());
			for (String s : m_tags.keySet())
			{
				out.writeObject(s);
				out.writeObject(m_tags.get(s));
			}
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
		{
			m_startPosition = in.readLong();
			m_endPosition = in.readLong();
			m_dataPointCount = in.readInt();
			m_dataType = (String)in.readObject();
			//m_dataPointCount = (int)((m_endPosition - m_startPosition) / DATA_POINT_SIZE);

			int tagCount = in.readInt();
			for (int I = 0; I < tagCount; I++)
			{
				String key = m_stringPool.getString((String)in.readObject());
				String value = m_stringPool.getString((String)in.readObject());
				m_tags.put(key, value);
			}
		}
	}

	//===========================================================================
	private class CachedDataPointRow implements DataPointRow
	{
		private long m_currentPosition;
		private long m_endPostition;
		private DataInputStream m_readBuffer = null;
		private Map<String, String> m_tags;
		private final String m_dataType;
		private final int m_dataPointCount;
		private int m_dataPointsRead = 0;

		public CachedDataPointRow(Map<String, String> tags,
				long startPosition, long endPostition, String dataType, int dataPointCount)
		{
			m_currentPosition = startPosition;
			m_endPostition = endPostition;

			m_tags = tags;
			m_dataType = dataType;
			m_dataPointCount = dataPointCount;
		}

		private void allocateReadBuffer()
		{
			int rowSize = (int) (m_endPostition - m_currentPosition);
			int bufferSize = (rowSize < m_maxReadBufferSize ? rowSize : m_maxReadBufferSize);

			m_readBuffer = new BufferedDataInputStream(m_randomAccessFile, m_currentPosition, bufferSize);
		}


		@Override
		public boolean hasNext()
		{
			return (m_dataPointsRead < m_dataPointCount);
			//return (m_readBuffer.hasRemaining() || m_currentPosition < m_endPostition);
		}

		@Override
		public DataPoint next()
		{
			DataPoint ret = null;

			try
			{
				//Lazy allocation of buffer to conserve memory when using group by's
				if (m_readBuffer == null)
					allocateReadBuffer();

				long timestamp = m_readBuffer.readLong();

				ret = m_dataPointFactory.createDataPoint(m_dataType, timestamp, m_readBuffer);

			}
			catch (IOException ioe)
			{
				logger.error("Error reading next data point.", ioe);
			}

			m_dataPointsRead ++;

			//Clean up buffer.  In cases where we are grouping not all rows are read
			//at once so this will save memory
			if (m_dataPointsRead == m_dataPointCount)
			{
				try
				{
					m_readBuffer.close();
					m_readBuffer = null;
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}
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
		public String getDatastoreType()
		{
			return m_dataType;
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

		@Override
		public int getDataPointCount()
		{
			return m_dataPointCount;
		}

		@Override
		public String toString()
		{
			return "CachedDataPointRow{" +
					"m_metricName='" + m_metricName + '\'' +
					", m_tags=" + m_tags +
					'}';
		}
	}
}
