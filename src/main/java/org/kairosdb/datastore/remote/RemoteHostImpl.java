package org.kairosdb.datastore.remote;

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
import org.kairosdb.core.exception.DatastoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;

import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;

public class RemoteHostImpl implements RemoteHost
{
	private static final Logger logger = LoggerFactory.getLogger(RemoteHostImpl.class);
	private static final String REMOTE_URL_PROP = "kairosdb.datastore.remote.remote_url";

	private final String url;
	private CloseableHttpClient client;

	@Inject
	public RemoteHostImpl(@Named(REMOTE_URL_PROP) String remoteUrl)
	{
		this.url = checkNotNullOrEmpty(remoteUrl, "url must not be null or empty");
		client = HttpClients.createDefault();
	}

	@Override
	public void sendZipFile(File zipFile) throws IOException
	{
		logger.debug("Sending {}", zipFile);
		HttpPost post = new HttpPost(url + "/api/v1/datapoints");

		FileInputStream zipStream = new FileInputStream(zipFile);
		post.setHeader("Content-Type", "application/gzip");

		post.setEntity(new InputStreamEntity(zipStream, zipFile.length()));
		try (CloseableHttpResponse response = client.execute(post))
		{

			zipStream.close();
			if (response.getStatusLine().getStatusCode() == 204)
			{
				try
				{
					Files.delete(zipFile.toPath());
				}
				catch (IOException e)
				{
					logger.error("Could not delete zip file: " + zipFile.getName());
				}
			}
			else
			{
				ByteArrayOutputStream body = new ByteArrayOutputStream();
				response.getEntity().writeTo(body);
				logger.error("Unable to send file " + zipFile + ": " + response.getStatusLine() +
						" - " + body.toString("UTF-8"));
			}
		}
	}

	@Override
	public void getKairosVersion() throws DatastoreException
	{
		try
		{
			HttpGet get = new HttpGet(url + "/api/v1/version");

			try (CloseableHttpResponse response = client.execute(get))
			{
				ByteArrayOutputStream bout = new ByteArrayOutputStream();
				response.getEntity().writeTo(bout);

				JSONObject respJson = new JSONObject(bout.toString("UTF-8"));

				logger.info("Connecting to remote Kairos version: " + respJson.getString("version"));
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
}
