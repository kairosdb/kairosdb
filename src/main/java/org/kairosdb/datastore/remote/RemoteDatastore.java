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

package org.kairosdb.datastore.remote;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.JSONException;
import org.json.JSONWriter;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datastore.CachedSearchResult;
import org.kairosdb.core.datastore.DataPointRow;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.datastore.DatastoreMetricQuery;
import org.kairosdb.core.exception.DatastoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

public class RemoteDatastore implements Datastore
{
	public static final Logger logger = LoggerFactory.getLogger(RemoteDatastore.class);
	public static final String DATA_DIR_PROP = "kairosdb.datastore.remote.data_dir";
	public static final String REMOTE_URL_PROP = "kairosdb.datastore.remote.remote_url";

	private final Object m_dataFileLock = new Object();
	private BufferedWriter m_dataWriter;
	private String m_dataFileName;
	private volatile boolean m_firstDataPoint = true;
	private String m_dataDirectory;
	private String m_remoteUrl;


	@Inject
	public RemoteDatastore(@Named(DATA_DIR_PROP) String dataDir,
			@Named(REMOTE_URL_PROP) String remoteUrl) throws IOException
	{
		m_dataDirectory = dataDir;
		m_remoteUrl = remoteUrl;

		sendAllZipfiles();
		openDataFile();
	}

	private void openDataFile() throws IOException
	{
		m_dataFileName = m_dataDirectory+"/"+System.currentTimeMillis();

		m_dataWriter = new BufferedWriter(new FileWriter(m_dataFileName));
		m_dataWriter.write("[\n");
		m_firstDataPoint = true;
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

	@Override
	public void putDataPoints(DataPointSet dps) throws DatastoreException
	{
		CharArrayWriter caw = new CharArrayWriter();
		JSONWriter writer = new JSONWriter(caw);
		try
		{
			writer.object();

			writer.key("name").value(dps.getName());
			writer.key("tags").object();
			Map<String, String> tags = dps.getTags();
			for (String tag : tags.keySet())
			{
				writer.key(tag).value(tags.get(tag));
			}
			writer.endObject();

			writer.key("datapoints").array();
			for (DataPoint dataPoint :dps.getDataPoints())
			{
				writer.array();
				writer.value(dataPoint.getTimestamp());
				if (dataPoint.isInteger())
					writer.value(dataPoint.getLongValue());
				else
					writer.value(dataPoint.getDoubleValue());
				writer.endArray();
			}
			writer.endArray();

			writer.endObject();
		}
		catch (JSONException e)
		{
			logger.error("Unable to write datapoints to CharArrayWriter", e);
		}

		synchronized (m_dataFileLock)
		{
			try
			{
				if (!m_firstDataPoint)
				{
					m_dataWriter.write(",\n");
				}
				m_firstDataPoint = false;

				caw.flush();
				caw.writeTo(m_dataWriter);
			}
			catch (IOException e)
			{
				logger.error("Unable to write datapoints: %s", caw.toString());
				logger.error("Unable to write datapoints to file", e);
			}
		}

	}

	/**
	 Sends a single zip file
	 @param zipFile Name of the zip file in the data directory.
	 @throws IOException
	 */
	private void sendZipfile(String zipFile) throws IOException
	{
		logger.debug("Sending %s", zipFile);
		HttpClient client = new DefaultHttpClient();
		HttpPost post = new HttpPost(m_remoteUrl+"/api/v1/datapoints");

		FileInputStream zipStream = new FileInputStream(new File(m_dataDirectory, zipFile));
		post.setHeader("Content-Type", "application/gzip");
		File zipFileObj = new File(zipFile);
		post.setEntity(new InputStreamEntity(zipStream, zipFileObj.length()));
		HttpResponse response = client.execute(post);

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
	 */
	private void zipFile(String file) throws IOException
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
	}


	public void sendData() throws IOException
	{
		String oldDataFile = m_dataFileName;

		synchronized (m_dataFileLock)
		{
			closeDataFile();
			openDataFile();
		}

		zipFile(oldDataFile);

		sendAllZipfiles();
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
	public List<DataPointRow> queryDatabase(DatastoreMetricQuery query, CachedSearchResult cachedSearchResult) throws DatastoreException
	{
		throw new DatastoreException("Method not implemented.");
	}

	@Override
	public void deleteDataPoints(DatastoreMetricQuery deleteQuery, CachedSearchResult cachedSearchResult) throws DatastoreException
	{
		throw new DatastoreException("Method not implemented.");
	}
}
