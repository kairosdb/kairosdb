package org.kairosdb.datastore.remote;

import com.google.inject.Inject;
import com.google.inject.name.Named;
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

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 5/29/13
 Time: 3:10 PM
 To change this template use File | Settings | File Templates.
 */
public class RemoteDatastore implements Datastore
{
	public static final String TEMP_DIR_PROP = "kairosdb.datastore.remote.tmp_dir";

	private Object m_cacheFileLock = new Object();
	private BufferedWriter m_cacheWriter;
	private String m_cacheFileName;
	private volatile boolean m_firstDataPoint = true;


	@Inject
	public RemoteDatastore(@Named(TEMP_DIR_PROP) String tempDir) throws IOException
	{
		m_cacheFileName = tempDir+"/"+System.currentTimeMillis();

		m_cacheWriter = new BufferedWriter(new FileWriter(m_cacheFileName));
		m_cacheWriter.write("[\n");
	}

	@Override
	public void close() throws InterruptedException, DatastoreException
	{
		try
		{
			m_cacheWriter.write("]");
			m_cacheWriter.flush();
			m_cacheWriter.close();
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

		synchronized (m_cacheFileLock)
		{
			try
			{
				if (!m_firstDataPoint)
				{
					m_cacheWriter.write(",\n");
				}
				m_firstDataPoint = false;

				caw.writeTo(m_cacheWriter);
			}
			catch (IOException e)
			{
				e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
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
