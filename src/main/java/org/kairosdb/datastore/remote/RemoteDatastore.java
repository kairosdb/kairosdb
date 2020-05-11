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

package org.kairosdb.datastore.remote;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.json.JSONException;
import org.json.JSONWriter;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.datastore.DatastoreMetricQuery;
import org.kairosdb.core.datastore.QueryCallback;
import org.kairosdb.core.datastore.TagSet;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.eventbus.FilterEventBus;
import org.kairosdb.eventbus.Publisher;
import org.kairosdb.eventbus.Subscribe;
import org.kairosdb.events.DataPointEvent;
import org.kairosdb.events.ShutdownEvent;
import org.kairosdb.util.DiskUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.SortedMap;
import java.util.zip.GZIPOutputStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;


public class RemoteDatastore implements Datastore
{
	private static final Logger logger = LoggerFactory.getLogger(RemoteDatastore.class);
	private static final String DATA_DIR_PROP = "kairosdb.datastore.remote.data_dir";
	private static final String DROP_PERCENT_PROP = "kairosdb.datastore.remote.drop_on_used_disk_space_threshold_percent";
	private static final String METRIC_PREFIX_FILTER = "kairosdb.datastore.remote.prefix_filter";

	private static final String FILE_SIZE_METRIC = "kairosdb.datastore.remote.file_size";
	private static final String ZIP_FILE_SIZE_METRIC = "kairosdb.datastore.remote.zip_file_size";
	private static final String WRITE_SIZE_METRIC = "kairosdb.datastore.remote.write_size";
	private static final String TIME_TO_SEND_METRIC = "kairosdb.datastore.remote.time_to_send";
	private static final String DELETE_ZIP_METRIC = "kairosdb.datastore.remote.deleted_zipFile_size";

	private final Object m_dataFileLock = new Object();
	private final Object m_sendLock = new Object();
	private final int m_dropPercent;
	private final File m_dataDirectory;
	private final RemoteHost m_remoteHost;
	private final DiskUtils m_diskUtils;
	private BufferedWriter m_dataWriter;
	private final Publisher<DataPointEvent> m_publisher;
	private String m_dataFileName;
	private volatile boolean m_firstDataPoint = true;
	private int m_dataPointCounter;

	private volatile Multimap<DataPointKey, DataPoint> m_dataPointMultimap;
	private final Object m_mapLock = new Object();  //Lock for the above map

	private boolean m_running;

	@Inject
	@Named("HOSTNAME")
	private String m_hostName = "localhost";

	private String[] m_prefixFilterArray = new String[0];

	@Inject
	private LongDataPointFactory m_longDataPointFactory = new LongDataPointFactoryImpl();

	@Inject(optional = true)
	public void setPrefixFilter(@Named(METRIC_PREFIX_FILTER) String prefixFilter)
	{
		if (!isNullOrEmpty(prefixFilter))
		{
			m_prefixFilterArray = prefixFilter.replaceAll("\\s+","").split(",");
			logger.info("List of metric prefixes to forward to remote KairosDB: " + Arrays.toString(m_prefixFilterArray));
		}
	}

	@Inject
	public RemoteDatastore(@Named(DATA_DIR_PROP) String dataDir,
			@Named(DROP_PERCENT_PROP) String dropPercent, RemoteHost remoteHost,
			FilterEventBus eventBus, DiskUtils diskUtils) throws IOException, DatastoreException
	{
		m_dataDirectory = new File(dataDir);
		m_dropPercent = Integer.parseInt(dropPercent);
		checkArgument(m_dropPercent > 0 && m_dropPercent <= 100, "drop_on_used_disk_space_threshold_percent must be greater than 0 and less than or equal to 100");
		checkNotNull(eventBus, "eventBus must not be null");
		m_remoteHost = checkNotNull(remoteHost, "remote host must not be null");
		m_publisher = eventBus.createPublisher(DataPointEvent.class);
		m_diskUtils = checkNotNull(diskUtils, "diskUtils must not be null");

		createNewMap();

		//This is to check and make sure the remote kairos is there and properly configured.
		remoteHost.getKairosVersion();

		sendAllZipfiles();
		openDataFile();
		m_running = true;

		Thread flushThread = new Thread(() -> {
			while (m_running)
			{
				try
				{
					flushMap();

					Thread.sleep(2000);
				}
				catch (Exception e)
				{
					logger.error("Error flushing map", e);
				}
			}
		});

		flushThread.setName("Remote flush");

		flushThread.start();

	}

	private Multimap<DataPointKey, DataPoint> createNewMap()
	{
		Multimap<DataPointKey, DataPoint> ret;
		synchronized (m_mapLock)
		{
			ret = m_dataPointMultimap;

			m_dataPointMultimap = ArrayListMultimap.create();
		}

		return ret;
	}

	private void flushMap()
	{
		Multimap<DataPointKey, DataPoint> flushMap = createNewMap();

		synchronized (m_dataFileLock)
		{
			try
			{
				try
				{
					for (DataPointKey dataPointKey : flushMap.keySet())
					{
						//We have to clear the writer every time or it gets confused
						//because we are only writing partial json each time.
						JSONWriter writer = new JSONWriter(m_dataWriter);

						if (!m_firstDataPoint)
						{
							m_dataWriter.write(",\n");
						}
						m_firstDataPoint = false;

						writer.object();

						writer.key("name").value(dataPointKey.getName());
						writer.key("ttl").value(dataPointKey.getTtl());
						writer.key("skip_validate").value(true);
						writer.key("tags").object();
						SortedMap<String, String> tags = dataPointKey.getTags();
						for (String tag : tags.keySet())
						{
							writer.key(tag).value(tags.get(tag));
						}
						writer.endObject();

						writer.key("datapoints").array();
						for (DataPoint dataPoint : flushMap.get(dataPointKey))
						{
							m_dataPointCounter++;
							writer.array();
							writer.value(dataPoint.getTimestamp());
							dataPoint.writeValueToJson(writer);
							writer.value(dataPoint.getApiDataType());
							writer.endArray();
						}
						writer.endArray();

						writer.endObject();
					}
				}
				catch (JSONException e)
				{
					logger.error("Unable to write datapoints to file", e);
				}

				m_dataWriter.flush();
			}
			catch (IOException e)
			{
				logger.error("Unable to write datapoints to file", e);
			}
		}
	}

	private void openDataFile() throws IOException
	{
		m_dataFileName = m_dataDirectory.getAbsolutePath() + File.separator + System.currentTimeMillis();

		m_dataWriter = new BufferedWriter(new FileWriter(m_dataFileName));
		m_dataWriter.write("[\n");
		m_firstDataPoint = true;
		m_dataPointCounter = 0;
	}

	private void closeDataFile() throws IOException
	{
		m_dataWriter.write("]");
		m_dataWriter.flush();
		m_dataWriter.close();
	}

	@Override
	public void close() throws InterruptedException, DatastoreException
	{
		try
		{
			m_running = false;
			flushMap();
			synchronized (m_dataFileLock)
			{
				closeDataFile();
			}

			zipFile(m_dataFileName);
			sendAllZipfiles();
		}
		catch (IOException e)
		{
			logger.error("Unable to send data files while closing down", e);
		}
	}

	@Subscribe
	public void shutdown(ShutdownEvent shutdownEvent)
	{
		try
		{
			close();
		}
		catch (InterruptedException | DatastoreException e)
		{
			logger.error("Remote shutdown failure", e);
		}
	}

	@Subscribe
	public void putDataPoint(DataPointEvent event) throws DatastoreException
	{
		String metricName = event.getMetricName();

		if (m_prefixFilterArray.length != 0)
		{
			boolean prefixMatch = false;
			for (String prefixFilter : m_prefixFilterArray)
			{
				if (metricName.startsWith(prefixFilter))
				{
					prefixMatch = true;
					break;
				}
			}

			if (!prefixMatch)
				return;
		}

		DataPointKey key = new DataPointKey(metricName, event.getTags(),
				event.getDataPoint().getApiDataType(), event.getTtl());

		synchronized (m_mapLock)
		{
			m_dataPointMultimap.put(key, event.getDataPoint());
		}
	}

	/**
	 Tries to send all zip files in the data directory.
	 */
	private void sendAllZipfiles() throws IOException
	{
		String[] zipFiles = m_dataDirectory.list((dir, name) -> (name.endsWith(".gz")));
		if (zipFiles == null)
			return;

		for (String zipFile : zipFiles)
		{
			try
			{
				m_remoteHost.sendZipFile(new File(m_dataDirectory, zipFile));
			}
			catch (IOException e)
			{
				logger.error("Unable to send data file " + zipFile);
				throw (e);
			}
		}
	}

	/**
	 Compresses the given file and removes the uncompressed file

	 @param file name of the zip file
	 @return Size of the zip file
	 */
	private long zipFile(String file) throws IOException
	{
		cleanDiskSpace();

		if (hasSpace())
		{
			String zipFile = file + ".gz";

			FileInputStream is = new FileInputStream(file);
			GZIPOutputStream gout = new GZIPOutputStream(new FileOutputStream(zipFile));

			byte[] buffer = new byte[1024];
			int readSize;
			while ((readSize = is.read(buffer)) != -1)
				gout.write(buffer, 0, readSize);

			is.close();
			gout.flush();
			gout.close();

			//delete uncompressed file
			new File(file).delete();

			return (new File(zipFile).length());
		}
		else
		{
			logger.error("No space available to create zip files after attempting clean up");
			return 0;
		}
	}

	private void cleanDiskSpace()
	{
		if (!hasSpace())
		{
			File[] zipFiles = m_dataDirectory.listFiles((dir, name) -> name.endsWith(".gz"));
			if (zipFiles != null && zipFiles.length > 0)
			{
				Arrays.sort(zipFiles, (f1, f2) -> Long.compare(f1.lastModified(), f2.lastModified()));
				try
				{
					long size = zipFiles[0].length();
					Files.delete(zipFiles[0].toPath());
					logger.warn("Disk is too full to create zip file. Deleted older zip file " + zipFiles[0].getName() + " size: " + size);
					// For forwarding this metric will be reported both on the local kairos node and the remote
					m_publisher.post(new DataPointEvent(DELETE_ZIP_METRIC, ImmutableSortedMap.of("host", m_hostName),
							m_longDataPointFactory.createDataPoint(System.currentTimeMillis(), size)));
					cleanDiskSpace(); // continue cleaning until space is freed up or all zip files are deleted.
				}
				catch (IOException e)
				{
					logger.error("Failed to delete zip file " + zipFiles[0].getAbsolutePath(), e);
				}
			}
		}
	}

	private boolean hasSpace()
	{
		return m_dropPercent >= 100 || m_diskUtils.percentAvailable(m_dataDirectory) < m_dropPercent;
	}

	void sendData() throws IOException
	{
		synchronized (m_sendLock)
		{
			String oldDataFile = m_dataFileName;
			long now = System.currentTimeMillis();

			long fileSize = (new File(m_dataFileName)).length();

			ImmutableSortedMap<String, String> tags = ImmutableSortedMap.<String, String>naturalOrder()
					.put("host", m_hostName)
					.build();

			int dataPointCounter;
			synchronized (m_dataFileLock)
			{
				closeDataFile();
				dataPointCounter = m_dataPointCounter;
				openDataFile();
			}

			long zipSize = zipFile(oldDataFile);

			sendAllZipfiles();

			long timeToSend = System.currentTimeMillis() - now;

			try
			{
				putDataPoint(new DataPointEvent(FILE_SIZE_METRIC, tags, m_longDataPointFactory.createDataPoint(now, fileSize), 0));
				putDataPoint(new DataPointEvent(WRITE_SIZE_METRIC, tags, m_longDataPointFactory.createDataPoint(now, dataPointCounter), 0));
				putDataPoint(new DataPointEvent(ZIP_FILE_SIZE_METRIC, tags, m_longDataPointFactory.createDataPoint(now, zipSize), 0));
				putDataPoint(new DataPointEvent(TIME_TO_SEND_METRIC, tags, m_longDataPointFactory.createDataPoint(now, timeToSend), 0));
			}
			catch (DatastoreException e)
			{
				logger.error("Error writing remote metrics", e);
			}
		}
	}


	@Override
	public Iterable<String> getMetricNames(String prefix) throws DatastoreException
	{
		throw new DatastoreException("Method not implemented.");
	}

	@Override
	public Iterable<String> getTagNames() throws DatastoreException
	{
		throw new DatastoreException("Method not implemented.");
	}

	@Override
	public Iterable<String> getTagValues() throws DatastoreException
	{
		throw new DatastoreException("Method not implemented.");
	}

	@Override
	public void queryDatabase(DatastoreMetricQuery query, QueryCallback queryCallback) throws DatastoreException
	{
		throw new DatastoreException("Method not implemented.");
	}

	@Override
	public void deleteDataPoints(DatastoreMetricQuery deleteQuery) throws DatastoreException
	{
		throw new DatastoreException("Method not implemented.");
	}

	@Override
	public TagSet queryMetricTags(DatastoreMetricQuery query) throws DatastoreException
	{
		throw new DatastoreException("Method not implemented.");
	}

	@Override
	public long queryCardinality(DatastoreMetricQuery query) throws DatastoreException
	{
		throw new DatastoreException("Method not implemented.");
	}
}
