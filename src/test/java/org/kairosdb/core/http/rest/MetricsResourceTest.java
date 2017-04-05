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
package org.kairosdb.core.http.rest;

import ch.qos.logback.classic.Level;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.junit.Test;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.testing.JsonResponse;
import org.kairosdb.util.LoggingUtils;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class MetricsResourceTest extends ResourceBase
{
	private static final String ADD_METRIC_URL = "http://localhost:9001/api/v1/datapoints";
	private static final String GET_METRIC_URL = "http://localhost:9001/api/v1/datapoints/query";
	private static final String METRIC_NAMES_URL = "http://localhost:9001/api/v1/metricnames";
	private static final String TAG_NAMES_URL = "http://localhost:9001/api/v1/tagnames";
	private static final String TAG_VALUES_URL = "http://localhost:9001/api/v1/tagvalues";

    @Test
	public void testAddEmptyBody() throws Exception
	{
		JsonResponse response = client.post("", ADD_METRIC_URL);

		assertResponse(response, 400, "{\"errors\":[\"Invalid json. No content due to end of input.\"]}");
	}

	@Test
	public void testAddSingleMetricLongValueSuccess() throws Exception
	{
		String json = Resources.toString(Resources.getResource("single-metric-long.json"), Charsets.UTF_8);

		JsonResponse response = client.post(json, ADD_METRIC_URL);

		assertResponse(response, 204);
	}

	@Test
	public void testAddSingleMetricDoubleValueSuccess() throws Exception
	{
		String json = Resources.toString(Resources.getResource("single-metric-double.json"), Charsets.UTF_8);

		JsonResponse response = client.post(json, ADD_METRIC_URL);

		assertResponse(response, 204);
	}

	@Test
	public void testAddMutipleDatapointSuccess() throws Exception
	{
		String json = Resources.toString(Resources.getResource("multiple-datapoints-metric.json"), Charsets.UTF_8);

		JsonResponse response = client.post(json, ADD_METRIC_URL);

		assertResponse(response, 204);
	}

	@Test
	public void testAddMultipleMetricLongValueSuccess() throws Exception
	{
		String json = Resources.toString(Resources.getResource("multi-metric-long.json"), Charsets.UTF_8);

		JsonResponse response = client.post(json, ADD_METRIC_URL);

		assertThat(response.getStatusCode(), equalTo(204));
	}

	@Test
	public void testAddMissingName() throws Exception
	{
		String json = Resources.toString(Resources.getResource("single-metric-missing-name.json"), Charsets.UTF_8);

		JsonResponse response = client.post(json, ADD_METRIC_URL);

		assertResponse(response, 400, "{\"errors\":[\"metric[0].name may not be empty.\"]}");
	}

	@Test
	public void testAddTimestampZeroValid() throws Exception
	{
		String json = Resources.toString(Resources.getResource("multi-metric-timestamp-zero.json"), Charsets.UTF_8);

		JsonResponse response = client.post(json, ADD_METRIC_URL);

		assertResponse(response, 204);
	}

	@Test
	public void testQuery() throws IOException
	{
		String json = Resources.toString(Resources.getResource("query-metric-absolute-dates.json"), Charsets.UTF_8);

		JsonResponse response = client.post(json, GET_METRIC_URL);

		assertResponse(response, 200,
				"{\"queries\":" +
						"[{\"sample_size\":10,\"results\":" +
						"[{\"name\":\"abc.123\",\"group_by\":[{\"name\":\"type\",\"type\":\"number\"}],\"tags\":{\"server\":[\"server1\",\"server2\"]},\"values\":[[1,60.2],[2,30.200000000000003],[3,20.1]]}]}]}");
	}

	@Test
	public void testQueryWithBeanValidationException() throws IOException
	{
		String json = Resources.toString(Resources.getResource("invalid-query-metric-relative-unit.json"), Charsets.UTF_8);

		JsonResponse response = client.post(json, GET_METRIC_URL);

		assertResponse(response, 400,
				"{\"errors\":[\"query.bogus is not a valid time unit, must be one of MILLISECONDS,SECONDS,MINUTES,HOURS,DAYS,WEEKS,MONTHS,YEARS\"]}");
	}

	@Test
	public void testQueryWithJsonMapperParsingException() throws IOException
	{
		String json = Resources.toString(Resources.getResource("invalid-query-metric-json.json"), Charsets.UTF_8);

		JsonResponse response = client.post(json, GET_METRIC_URL);

		assertResponse(response, 400,
				"{\"errors\":[\"com.google.gson.stream.MalformedJsonException: Use JsonReader.setLenient(true) to accept malformed JSON at line 2 column 22\"]}");
	}

	@Test
	public void testMetricNames() throws IOException
	{
		JsonResponse response = client.get(METRIC_NAMES_URL);

		assertResponse(response, 200, "{\"results\":[\"cpu\",\"memory\",\"disk\",\"network\"]}");
	}

	@Test
	public void testTagNames() throws IOException
	{
		JsonResponse response = client.get(TAG_NAMES_URL);

		assertResponse(response, 200, "{\"results\":[\"server1\",\"server2\",\"server3\"]}");
	}

	@Test
	public void testTagValues() throws IOException
	{
		JsonResponse response = client.get(TAG_VALUES_URL);

		assertResponse(response, 200, "{\"results\":[\"larry\",\"moe\",\"curly\"]}");
	}

	@Test
	public void test_datastoreThrowsException() throws DatastoreException, IOException
	{
		Level previousLogLevel = LoggingUtils.setLogLevel(Level.OFF);

		try
		{
			datastore.throwException(new DatastoreException("bogus"));

			String json = Resources.toString(Resources.getResource("query-metric-absolute-dates.json"), Charsets.UTF_8);

			JsonResponse response = client.post(json, GET_METRIC_URL);

			datastore.throwException(null);

			assertThat(response.getStatusCode(), equalTo(500));
			assertThat(response.getJson(), equalTo("{\"errors\":[\"org.kairosdb.core.exception.DatastoreException: bogus\"]}"));
			assertEquals(3, queuingManager.getAvailableThreads());
		}
		finally
		{
			LoggingUtils.setLogLevel(previousLogLevel);
		}
	}

	private void assertResponse(JsonResponse response, int responseCode, String expectedContent)
	{
		assertThat(response.getStatusCode(), equalTo(responseCode));
		assertThat(response.getHeader("Content-Type"), startsWith("application/json"));
		assertThat(response.getJson(), equalTo(expectedContent));
	}

	private void assertResponse(JsonResponse response, int responseCode)
	{
		assertThat(response.getStatusCode(), equalTo(responseCode));
		assertThat(response.getHeader("Content-Type"), startsWith("application/json"));
		assertThat(response.getStatusString(), equalTo("No Content"));
	}
}