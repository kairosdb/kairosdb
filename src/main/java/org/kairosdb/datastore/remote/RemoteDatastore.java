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
import com.google.common.eventbus.Subscribe;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONWriter;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.datastore.DatastoreMetricQuery;
import org.kairosdb.core.datastore.QueryCallback;
import org.kairosdb.core.datastore.TagSet;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.events.DataPointEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.SortedMap;
import java.util.zip.GZIPOutputStream;


public class RemoteDatastore implements Datastore
{
	public static final Logger logger = LoggerFactory.getLogger(RemoteDatastore.class);
	public static final String DATA_DIR_PROP = "kairosdb.datastore.remote.data_dir";
	public static final String REMOTE_URL_PROP = "kairosdb.datastore.remote.remote_url";
	public static final String METRIC_PREFIX_FILTER = "kairosdb.datastore.remote.prefix_filter";

	public static final String FILE_SIZE_METRIC = "kairosdb.datastore.remote.file_size";
	public static final String ZIP_FILE_SIZE_METRIC = "kairosdb.datastore.remote.zip_file_size";
	public static final String WRITE_SIZE_METRIC = "kairosdb.datastore.remote.write_size";
	public static final String TIME_TO_SEND_METRIC = "kairosdb.datastore.remote.time_to_send";

	private final Object m_dataFileLock = new Object();
	private final Object m_sendLock = new Object();
	private BufferedWriter m_dataWriter;
	private String m_dataFileName;
	private volatile boolean m_firstDataPoint = true;
	private String m_dataDirectory;
	private String m_remoteUrl;
	private int m_dataPointCounter;

	private volatile Multimap<DataPointKey, DataPoint> m_dataPointMultimap;
	private Object m_mapLock = new Object();  //Lock for the above map

	private CloseableHttpClient m_client;
	private boolean m_running;

	@Inject
	@Named("HOSTNAME")
	private String m_hostName = "localhost";

	@Inject(optional = true)
	@Named(METRIC_PREFIX_FILTER)
	private String m_prefixFilter = null;

	@Inject
	private LongDataPointFactory m_longDataPointFactory = new LongDataPointFactoryImpl();


	@Inject
	public RemoteDatastore(@Named(DATA_DIR_PROP) String dataDir,
			@Named(REMOTE_URL_PROP) String remoteUrl) throws IOException, DatastoreException
	{
		m_dataDirectory = dataDir;
		m_remoteUrl = remoteUrl;
		m_client = HttpClients.createDefault();

		createNewMap();

		//This is to check and make sure the remote kairos is there and properly configured.
		getKairosVersion();

		sendAllZipfiles();
		openDataFile();
		m_running = true;

		Thread flushThread = new Thread(new Runnable()
		{
			@Override
			public void run()
			{
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
			}
		});

		flushThread.start();

	}

	private Multimap<DataPointKey, DataPoint> createNewMap()
	{
		Multimap<DataPointKey, DataPoint> ret;
		synchronized (m_mapLock)
		{
			ret = m_dataPointMultimap;

			m_dataPointMultimap = ArrayListMultimap.<DataPointKey, DataPoint>create();
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
							m_dataPointCounter ++;
							writer.array();
							writer.value(dataPoint.getTimestamp());
							dataPoint.writeValueToJson(writer);
							writer.value(dataPoint.getApiDataType());
							/*if (dataPoint.isLong())
								writer.value(dataPoint.getLongValue());
							else
								writer.value(dataPoint.getDoubleValue());*/
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

	private void getKairosVersion() throws DatastoreException
	{
		try
		{
			HttpGet get = new HttpGet(m_remoteUrl+"/api/v1/version");

			try(CloseableHttpResponse response = m_client.execute(get))
			{
				ByteArrayOutputStream bout = new ByteArrayOutputStream();
				response.getEntity().writeTo(bout);
	
				JSONObject respJson = new JSONObject(bout.toString("UTF-8"));
	
				logger.info("Connecting to remote Kairos version: "+ respJson.getString("version"));
			}
		}
		catch (IOException e)
		{
			throw new DatastoreException("Unable to connect to remote kairos node.", e);
		}
		catch (JSONException e)
		{
			throw new DatastoreException("Unable to parse response from remote kairos node.", e);
		}
	}

	private void openDataFile() throws IOException
	{
		m_dataFileName = m_dataDirectory+"/"+System.currentTimeMillis();

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
	public void putDataPoint(DataPointEvent event) throws DatastoreException
	{
		if ((m_prefixFilter != null) && (!event.getMetricName().startsWith(m_prefixFilter)))
			return;

		DataPointKey key = new DataPointKey(event.getMetricName(), event.getTags(),
				event.getDataPoint().getApiDataType(), event.getTtl());

		synchronized (m_mapLock)
		{
			m_dataPointMultimap.put(key, event.getDataPoint());
		}
	}

	/**
	 Sends a single zip file
	 @param zipFile Name of the zip file in the data directory.
	 @throws IOException
	 */
	private void sendZipfile(String zipFile) throws IOException
	{
		logger.debug("Sending {}", zipFile);
		HttpPost post = new HttpPost(m_remoteUrl+"/api/v1/datapoints");

		File zipFileObj = new File(m_dataDirectory, zipFile);
		FileInputStream zipStream = new FileInputStream(zipFileObj);
		post.setHeader("Content-Type", "application/gzip");
		
		post.setEntity(new InputStreamEntity(zipStream, zipFileObj.length()));
		try(CloseableHttpResponse response = m_client.execute(post))
		{

			zipStream.close();
			if (response.getStatusLine().getStatusCode() == 204)
			{
				zipFileObj.delete();
			}
			else
			{
				ByteArrayOutputStream body = new ByteArrayOutputStream();
				response.getEntity().writeTo(body);
				logger.error("Unable to send file " + zipFile + ": " + response.getStatusLine() +
						" - "+ body.toString("UTF-8"));
			}
		}
	}

	/**
	 Tries to send all zip files in the data directory.
	 */
	private void sendAllZipfiles() throws IOException
	{
		File dataDirectory = new File(m_dataDirectory);

		String[] zipFiles = dataDirectory.list(new FilenameFilter()
				{
					@Override
					public boolean accept(File dir, String name)
					{
						return (name.endsWith(".gz"));
					}
				});
		if(zipFiles == null)
			return;

		for (String zipFile : zipFiles)
		{
			try
			{
				sendZipfile(zipFile);
			}
			catch (IOException e)
			{
				logger.error("Unable to send data file "+zipFile);
				throw (e);
			}
		}
	}


	/**
	 Compresses the given file and removes the uncompressed file
	 @param file
	 @return Size of the zip file
	 */
	private long zipFile(String file) throws IOException
	{
		String zipFile = file+".gz";

		FileInputStream is = new FileInputStream(file);
		GZIPOutputStream gout = new GZIPOutputStream(new FileOutputStream(zipFile));

		byte[] buffer = new byte[1024];
		int readSize = 0;
		while ((readSize = is.read(buffer)) != -1)
			gout.write(buffer, 0, readSize);

		is.close();
		gout.flush();
		gout.close();

		//delete uncompressed file
		new File(file).delete();

		return (new File(zipFile).length());
	}


	public void sendData() throws IOException
	{
		synchronized (m_sendLock)
		{
			String oldDataFile = m_dataFileName;
			long now = System.currentTimeMillis();

			long fileSize = (new File(m_dataFileName)).length();

			ImmutableSortedMap<String, String> tags = ImmutableSortedMap.<String, String>naturalOrder()
					.put("host", m_hostName)
					.build();

			synchronized (m_dataFileLock)
			{
				closeDataFile();
				openDataFile();
			}

			long zipSize = zipFile(oldDataFile);

			sendAllZipfiles();

			long timeToSend = System.currentTimeMillis() - now;

			try
			{
				putDataPoint(new DataPointEvent(FILE_SIZE_METRIC, tags, m_longDataPointFactory.createDataPoint(now, fileSize), 0));
				putDataPoint(new DataPointEvent(WRITE_SIZE_METRIC, tags, m_longDataPointFactory.createDataPoint(now, m_dataPointCounter), 0));
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
	public Iterable<String> getMetricNames() throws DatastoreException
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
	public void setValue(String service, String serviceKey, String key, String value) throws DatastoreException
	{
		throw new DatastoreException("Method not implemented.");
	}

	@Override
	public String getValue(String service, String serviceKey, String key) throws DatastoreException
	{
		throw new DatastoreException("Method not implemented.");
	}

    @Override
    public Iterable<String> listServiceKeys(String service)
            throws DatastoreException
    {
        throw new DatastoreException("Method not implemented.");
    }

    @Override
	public Iterable<String> listKeys(String service, String serviceKey) throws DatastoreException
	{
		throw new DatastoreException("Method not implemented.");
	}

	@Override
	public Iterable<String> listKeys(String service, String serviceKey, String keyStartsWith) throws DatastoreException
	{
		throw new DatastoreException("Method not implemented.");
	}

    @Override
    public void deleteKey(String service, String serviceKey, String key)
            throws DatastoreException
    {
    }
}
