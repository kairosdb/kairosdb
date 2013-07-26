package org.kairosdb.testclient;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.apache.commons.io.IOUtils;
import sun.net.httpserver.DefaultHttpServerProvider;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

import static org.testng.AssertJUnit.assertEquals;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 7/24/13
 Time: 1:49 PM
 To change this template use File | Settings | File Templates.
 */

/*
List of tests we need to perform
1. test with a range that returns no data
2. test that returns data
3. test that will use the cache file to return data.
4. Nice to verify no open file handles after tests have ran.
 */

public class QueryTests
{
	private JsonParser m_parser = new JsonParser();

	private JsonElement readJsonFromStream(String path) throws IOException, JSONException
	{
		InputStream is = ClassLoader.getSystemResourceAsStream(path);
		if (is == null)
			return (null);

		String str = IOUtils.toString(is);
		return (m_parser.parse(str));
	}

	private JsonElement postQuery(JsonElement query) throws IOException, JSONException
	{
		HttpClient client = new DefaultHttpClient();
		HttpPost post = new HttpPost("http://localhost:8080/api/v1/datapoints/query");
		post.setHeader("Content-Type", "application/json");

		post.setEntity(new StringEntity(query.toString()));
		HttpResponse httpResponse = client.execute(post);

		if (httpResponse.getStatusLine().getStatusCode() != 200)
		{
			httpResponse.getEntity().writeTo(System.out);
			return (null);
		}

		return (m_parser.parse(IOUtils.toString(httpResponse.getEntity().getContent())));
	}


	@DataProvider(name = "query-provider")
	public Object[][] getQueryTests() throws IOException, JSONException
	{
		ArrayList<Object[]> ret = new ArrayList<Object[]>();

		ClassLoader cl = this.getClass().getClassLoader();

		for (int test = 1;; test++)
		{
			JsonElement query = readJsonFromStream("tests/test_" + test + "/query.json");
			JsonElement response = readJsonFromStream("tests/test_"+ test + "/response.json");

			if (query == null)
				break;

			ret.add(new Object[]{query, response});
		}

		return (ret.toArray(new Object[0][]));
	}


	@Test(dataProvider = "query-provider")
	public void performQueryTest(JsonElement query, JsonElement response) throws IOException, JSONException
	{
		JsonElement serverResponse = postQuery(query);

		assertEquals(response, serverResponse);
	}
}
