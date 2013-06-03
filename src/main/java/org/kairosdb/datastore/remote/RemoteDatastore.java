package org.kairosdb.datastore.remote;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
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

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 5/29/13
 Time: 3:10 PM
 To change this template use File | Settings | File Templates.
 */
public class RemoteDatastore implements Datastore
{
	public static final String DATA_DIR_PROP = "kairosdb.datastore.remote.data_dir";
	public static final String REMOTE_URL_PROP = "kairosdb.datastore.remote.remote_url";

	private Object m_dataFileLock = new Object();
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

		//TODO: send any data in directory before starting
		openDataFile();
	}

	private void openDataFile() throws IOException
	{
		m_dataFileName = m_dataDirectory+"/"+System.currentTimeMillis();

		m_dataWriter = new BufferedWriter(new FileWriter(m_dataFileName));
		m_dataWriter.write("[\n");
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
		}
		catch (IOException e)
		{
			e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
		}
		//Close json and send it off
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
			e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
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

				caw.writeTo(m_dataWriter);
			}
			catch (IOException e)
			{
				e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
			}
		}

	}

	public void sendData() throws IOException
	{
		String oldDataFile = m_dataFileName;

		synchronized (m_dataFileLock)
		{
			closeDataFile();
			openDataFile();
		}

		HttpClient client = new DefaultHttpClient();
		HttpPost post = new HttpPost(m_remoteUrl+"/api/v1/datapoints");

		GZIPInputStream zipStream = new GZIPInputStream(new FileInputStream(oldDataFile));
		post.setHeader("Content-Type", "application/gzip");
		post.setEntity(new InputStreamEntity(zipStream, -1));
		HttpResponse response = client.execute(post);

		if (response.getStatusLine().getStatusCode() == 200)
		{
			new File(oldDataFile).delete();
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
	public List<DataPointRow> queryDatabase(DatastoreMetricQuery query, CachedSearchResult cachedSearchResult) throws DatastoreException
	{
		throw new DatastoreException("Method not implemented.");
	}

	@Override
	public void deleteDataPoints(DatastoreMetricQuery deleteQuery) throws DatastoreException
	{
		throw new DatastoreException("Method not implemented.");
	}
}
