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

package org.kairosdb.testclient;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.JSONException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkState;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;


/*
List of tests we need to perform
1. test with a range that returns no data
2. test that returns data
3. test that will use the cache file to return data.
4. Nice to verify no open file handles after tests have ran.
5. Test aggregators (multiple)
6. Test group by
 */

public class QueryTests
{
	private JsonParser m_parser = new JsonParser();
	private String m_host = "127.0.0.1";
	private String m_port = "8080";

	@Parameters({"host", "port"})
	public QueryTests(String host, String port)
	{
		m_host = host;
		m_port = port;
	}

	private JsonElement readJsonFromStream(String path, String metricName) throws IOException, JSONException
	{
		InputStream is = ClassLoader.getSystemResourceAsStream(path);
		if (is == null)
			return (null);

		String str = IOUtils.toString(is);

		//replace metric name
		str = str.replace("<metric_name>", metricName);

		return (m_parser.parse(str));
	}

	private JsonElement postQuery(JsonElement query) throws IOException, JSONException
	{
		HttpClient client = new DefaultHttpClient();
		HttpPost post = new HttpPost("http://" + m_host + ":" + m_port + "/api/v1/datapoints/query");
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

	private int putDataPoints(JsonElement dataPoints) throws IOException, JSONException
	{
		HttpClient client = new DefaultHttpClient();
		HttpPost post = new HttpPost("http://" + m_host + ":" + m_port + "/api/v1/datapoints");
		post.setHeader("Content-Type", "application/json");

		post.setEntity(new StringEntity(dataPoints.toString()));
		HttpResponse httpResponse = client.execute(post);

		return httpResponse.getStatusLine().getStatusCode();
	}

	private int deleteDataPoints(JsonElement query) throws IOException, JSONException
	{
		HttpClient client = new DefaultHttpClient();
		HttpPost post = new HttpPost("http://" + m_host + ":" + m_port + "/api/v1/datapoints/delete");
		post.setHeader("Content-Type", "application/json");

		post.setEntity(new StringEntity(query.toString()));
		HttpResponse httpResponse = client.execute(post);

		return httpResponse.getStatusLine().getStatusCode();
	}

	@DataProvider(name = "query-provider")
	public Object[][] getQueryTests() throws IOException, JSONException, URISyntaxException
	{
		ArrayList<Object[]> ret = new ArrayList<Object[]>();

		List<String> resourceDirectoryNames = getTestDirectories("tests");

		for (String resourceDirectory : resourceDirectoryNames)
		{
			String metricName = "integration_test_" + UUID.randomUUID();
			JsonElement dataPoints = readJsonFromStream("tests/" + resourceDirectory + "/datapoints.json", metricName);
			JsonElement query = readJsonFromStream("tests/" + resourceDirectory + "/query.json", metricName);
			JsonElement response = readJsonFromStream("tests/" + resourceDirectory + "/response.json", metricName);

			checkState(query != null, "No query found for test " + resourceDirectory);

			ret.add(new Object[]{resourceDirectory, dataPoints, query, response});
		}

		return (ret.toArray(new Object[0][]));
	}

	@Test(dataProvider = "query-provider")
	public void performQueryTest(String testName, JsonElement dataPoints, JsonElement query, JsonElement response)
			throws IOException, JSONException, InterruptedException
	{
		int retryCount = 0;
		if (dataPoints != null)
		{
			int status = putDataPoints(dataPoints);
			assertThat(status, equalTo(204));
			retryCount = 3;
		}

		do
		{
			try
			{
				JsonElement serverResponse = postQuery(query);
				assertResponse(testName, serverResponse, response);
				break;
			}
			catch (AssertionError e)
			{
				if (retryCount == 0)
					throw e;

				retryCount--;
				Thread.sleep(500); // Need to wait until datapoints are available in the data store
			}
		} while (true);

		if (dataPoints != null)
		{
			// clean up
			int status = deleteDataPoints(query);

			assertThat(status, equalTo(204));

			// todo verify that data points are gone
		}
	}

	private void assertResponse(String testName, JsonElement actual, JsonElement expected)
	{
		JsonArray actualQueries = actual.getAsJsonObject().get("queries").getAsJsonArray();
		JsonArray expectedQueries = expected.getAsJsonObject().get("queries").getAsJsonArray();

		assertThat("Number of queries is different for test: " + testName, actualQueries.size(), equalTo(expectedQueries.size()));

		for (int i = 0; i < expectedQueries.size(); i++)
		{
			JsonArray actualResult = actualQueries.get(i).getAsJsonObject().get("results").getAsJsonArray();
			JsonArray expectedResult = expectedQueries.get(i).getAsJsonObject().get("results").getAsJsonArray();

			assertThat("Number of results is different for test: " + testName, actualResult.size(), equalTo(expectedResult.size()));

			for (int j = 0; j < expectedResult.size(); j++)
			{
				JsonObject actualMetric = actualResult.get(j).getAsJsonObject();
				JsonObject expectedMetric = expectedResult.get(j).getAsJsonObject();

				assertThat("Metric name is different for test: " + testName, actualMetric.get("name"), equalTo(expectedMetric.get("name")));
				assertTags(testName, actualMetric, expectedMetric);
				assertDataPoints(testName, i, j, actualMetric, expectedMetric);
			}
		}
	}

	private void assertTags(String testName, JsonObject actual, JsonObject expected)
	{
		JsonObject actualTags = actual.getAsJsonObject("tags");
		JsonObject expectedTags = expected.getAsJsonObject("tags");

		for (Map.Entry<String, JsonElement> tag : expectedTags.entrySet())
		{
			String tagName = tag.getKey();
			assertThat("Missing tag: " + tagName + " for test " + testName, actualTags.has(tagName), equalTo(true));
			assertThat("Tag value different for key: " + tagName + " for test:" + testName, actualTags.get(tagName), equalTo(tag.getValue()));
		}
	}

	private void assertDataPoints(String testName, int queryCount, int resultCount, JsonObject actual, JsonObject expected)
	{
		JsonArray actualValues = actual.getAsJsonArray("values");
		JsonArray expectedValues = expected.getAsJsonArray("values");

		assertThat(String.format("Number of datapoints is different for test %s, query: %d, result: %d",
				testName, queryCount, resultCount),
				actualValues.size(), equalTo(expectedValues.size()));

		for (int i = 0; i < expectedValues.size(); i++)
		{
			assertThat(String.format("Timestamps different for data point %d for test %s, query: %d, result: %d",
					i, testName, queryCount, resultCount),
					actualValues.get(i).getAsJsonArray().get(0), equalTo(expectedValues.get(i).getAsJsonArray().get(0)));


			if (isDouble(actualValues.get(i).getAsJsonArray().get(1)))
				assertThat(String.format("Values different for data point: %d for test %s, query: %d, result: %d",
						i, testName, queryCount, resultCount),
						actualValues.get(i).getAsJsonArray().get(1).getAsDouble(),
						closeTo(expectedValues.get(i).getAsJsonArray().get(1).getAsDouble(), .01));
			else
				assertThat(String.format("Values different for data point: %d for test: %s, query: %d, result: %d",
						i, testName, queryCount, resultCount),
						actualValues.get(i).getAsJsonArray().get(1), equalTo(expectedValues.get(i).getAsJsonArray().get(1)));
		}
	}

	private boolean isDouble(JsonElement value)
	{
		return value.toString().contains(".");
	}

	public static List<String> getTestDirectories(String matchingDirectoryName) throws URISyntaxException, IOException
	{
		return findTestDirectories(new File("src/integration-test/resources"), matchingDirectoryName);
	}

	@SuppressWarnings("ConstantConditions")
	private static List<String> findTestDirectories(File directory, String matchingDirectoryName)
	{
		List<String> matchingDirectories = new ArrayList<String>();

		for (File file : directory.listFiles())
		{
			if (file.isDirectory())
			{
				if (file.getParentFile().getName().equals(matchingDirectoryName))
				{
					matchingDirectories.add(file.getName());
				}
				else
					matchingDirectories.addAll(findTestDirectories(file, matchingDirectoryName));
			}
		}
		return matchingDirectories;
	}
}
