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
package org.kairosdb.core.http.rest;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.google.inject.*;
import com.google.inject.name.Names;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointListener;
import org.kairosdb.core.DataPointListenerProvider;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.aggregator.AggregatorFactory;
import org.kairosdb.core.aggregator.TestAggregatorFactory;
import org.kairosdb.core.datastore.*;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.groupby.GroupByFactory;
import org.kairosdb.core.groupby.TestGroupByFactory;
import org.kairosdb.core.http.WebServer;
import org.kairosdb.core.http.WebServletModule;
import org.kairosdb.core.http.rest.json.GsonParser;
import org.kairosdb.testing.Client;
import org.kairosdb.testing.JsonResponse;
import org.kairosdb.testing.TestingDataPointRowImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertThat;

public class MetricsResourceTest
{
	private static final String ADD_METRIC_URL = "http://localhost:9001/api/v1/datapoints";
	private static final String GET_METRIC_URL = "http://localhost:9001/api/v1/datapoints/query";
	private static final String METRIC_NAMES_URL = "http://localhost:9001/api/v1/metricnames";
	private static final String TAG_NAMES_URL = "http://localhost:9001/api/v1/tagnames";
	private static final String TAG_VALUES_URL = "http://localhost:9001/api/v1/tagvalues";

	private static Client client;
	private static WebServer server;

	@BeforeClass
	public static void startup() throws Exception
	{
		Injector injector = Guice.createInjector(new WebServletModule(new Properties()), new AbstractModule()
		{
			private Datastore datastore = new TestDatastore();

			@Override
			protected void configure()
			{
				bind(Integer.class).annotatedWith(Names.named(WebServer.JETTY_PORT_PROPERTY)).toInstance(9001);
				bind(String.class).annotatedWith(Names.named(WebServer.JETTY_WEB_ROOT_PROPERTY)).toInstance("bogus");
				bind(Datastore.class).toInstance(datastore);
				bind(KairosDatastore.class).in(Singleton.class);
				bind(AggregatorFactory.class).to(TestAggregatorFactory.class);
				bind(GroupByFactory.class).to(TestGroupByFactory.class);
				bind(GsonParser.class).in(Singleton.class);
				bind(new TypeLiteral<List<DataPointListener>>(){}).toProvider(DataPointListenerProvider.class);
				bind(QueryQueuingManager.class).in(Singleton.class);
				bindConstant().annotatedWith(Names.named("HOSTNAME")).to("HOST");
				bindConstant().annotatedWith(Names.named("kairosdb.datastore.concurrentQueryThreads")).to(1);
			}
		});
		server = injector.getInstance(WebServer.class);
		server.start();

		client = new Client();
	}

	@AfterClass
	public static void tearDown() throws Exception
	{
		if (server != null)
		{
			server.stop();
		}
	}

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
	public void testAddInvalidTimestamp() throws Exception
	{
		String json = Resources.toString(Resources.getResource("multi-metric-invalid-timestamp.json"), Charsets.UTF_8);

		JsonResponse response = client.post(json, ADD_METRIC_URL);

		assertResponse(response, 400, "{\"errors\":[\"datapoints.timestamp must be greater than or equal to 1\"]}");
	}

	@Test
	public void testQuery() throws IOException
	{
		String json = Resources.toString(Resources.getResource("query-metric-absolute-dates.json"), Charsets.UTF_8);

		JsonResponse response = client.post(json, GET_METRIC_URL);

		assertResponse(response, 200,
				"{\"queries\":" +
						"[{\"results\":" +
						"[{\"name\":\"abc.123\",\"tags\":{\"server\":[\"server1\",\"server2\"]},\"values\":[[1,60.2],[2,30.200000000000003],[3,20.1]]}]}]}");
	}

	@Test
	public void testQueryWithBeanValidationException() throws IOException
	{
		String json = Resources.toString(Resources.getResource("query-metric-invalid-relative-unit.json"), Charsets.UTF_8);

		JsonResponse response = client.post(json, GET_METRIC_URL);

		assertResponse(response, 400,
				"{\"errors\":[\"\\\"bogus\\\" is not a valid time unit, must be one of MILLISECONDS,SECONDS,MINUTES,HOURS,DAYS,WEEKS,MONTHS,YEARS\"]}");
	}

	@Test
	public void testQueryWithJsonMapperParsingException() throws IOException
	{
		String json = Resources.toString(Resources.getResource("query-metric-invalid-json.json"), Charsets.UTF_8);

		JsonResponse response = client.post(json, GET_METRIC_URL);

		assertResponse(response, 400,
				"{\"errors\":[\"com.google.gson.stream.MalformedJsonException: Expected EOF at line 2 column 21\"]}");
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

	public static class TestDatastore implements Datastore
	{

		protected TestDatastore() throws DatastoreException
		{
		}

		@Override
		public void close() throws InterruptedException
		{
		}

		@Override
		public void putDataPoints(DataPointSet dps)
		{

		}

		@Override
		public Iterable<String> getMetricNames()
		{
			return Arrays.asList("cpu", "memory", "disk", "network");
		}

		@Override
		public Iterable<String> getTagNames()
		{
			return Arrays.asList("server1", "server2", "server3");
		}

		@Override
		public Iterable<String> getTagValues()
		{
			return Arrays.asList("larry", "moe", "curly");
		}

		@Override
		public List<DataPointRow> queryDatabase(DatastoreMetricQuery query, CachedSearchResult cachedSearchResult)
		{
			List<DataPointRow> groups = new ArrayList<DataPointRow>();

			TestingDataPointRowImpl group1 = new TestingDataPointRowImpl();
			group1.setName(query.getName());
			group1.addDataPoint(new DataPoint(1, 10));
			group1.addDataPoint(new DataPoint(1, 20));
			group1.addDataPoint(new DataPoint(2, 10));
			group1.addDataPoint(new DataPoint(2, 5));
			group1.addDataPoint(new DataPoint(3, 10));

			group1.addTag("server", "server1");

			groups.add(group1);

			TestingDataPointRowImpl group2 = new TestingDataPointRowImpl();
			group2.setName(query.getName());
			group2.addDataPoint(new DataPoint(1, 10.1));
			group2.addDataPoint(new DataPoint(1, 20.1));
			group2.addDataPoint(new DataPoint(2, 10.1));
			group2.addDataPoint(new DataPoint(2, 5.1));
			group2.addDataPoint(new DataPoint(3, 10.1));

			group2.addTag("server", "server2");

			groups.add(group2);

			return groups;
		}

		@Override
		public void deleteDataPoints(DatastoreMetricQuery deleteQuery, CachedSearchResult cachedSearchResult) throws DatastoreException
		{
		}
	}

}