// OpenTSDB2
// Copyright (C) 2013 Proofpoint, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>
package net.opentsdb.core.http.rest;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;
import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPointSet;
import net.opentsdb.core.aggregator.AggregatorFactory;
import net.opentsdb.core.aggregator.TestAggregatorFactory;
import net.opentsdb.core.datastore.*;
import net.opentsdb.core.exception.DatastoreException;
import net.opentsdb.core.http.WebServer;
import net.opentsdb.core.http.WebServletModule;
import net.opentsdb.testing.JsonResponse;
import net.opentsdb.testing.TestingDataPointRowImpl;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertThat;

public class MetricsResourceTest
{
	private static final String ADD_METRIC_URL = "http://localhost:9000/api/v1/datapoints";
	private static final String GET_METRIC_URL = "http://localhost:9000/api/v1/datapoints/query";
	private static final String METRIC_NAMES_URL = "http://localhost:9000/api/v1/metricnames";
	private static final String TAG_NAMES_URL = "http://localhost:9000/api/v1/tagnames";
	private static final String TAG_VALUES_URL = "http://localhost:9000/api/v1/tagvalues";

	private static HttpClient client;
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
				bind(Integer.class).annotatedWith(Names.named(WebServer.JETTY_PORT_PROPERTY)).toInstance(9000);
				bind(String.class).annotatedWith(Names.named(WebServer.JETTY_WEB_ROOT_PROPERTY)).toInstance("bogus");
				bind(Datastore.class).toInstance(datastore);
				bind(AggregatorFactory.class).to(TestAggregatorFactory.class);
			}
		});
		server = injector.getInstance(WebServer.class);
		server.start();

		client = new DefaultHttpClient();
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
		JsonResponse response = post("", ADD_METRIC_URL);

		assertResponse(response, 400, "{\"errors\":[\"Invalid json for Java type MetricRequestList:No content to map to Object due to end of input\"]}");
	}

	@Test
	public void testAddSingleMetricLongValueSuccess() throws Exception
	{
		String json = Resources.toString(Resources.getResource("single-metric-long.json"), Charsets.UTF_8);

		JsonResponse response = post(json, ADD_METRIC_URL);

		assertResponse(response, 204);
	}

	@Test
	public void testAddSingleMetricDoubleValueSuccess() throws Exception
	{
		String json = Resources.toString(Resources.getResource("single-metric-double.json"), Charsets.UTF_8);

		JsonResponse response = post(json, ADD_METRIC_URL);

		assertResponse(response, 204);
	}

	@Test
	public void testAddMutipleDatapointSuccess() throws Exception
	{
		String json = Resources.toString(Resources.getResource("multiple-datapoints-metric.json"), Charsets.UTF_8);

		JsonResponse response = post(json, ADD_METRIC_URL);

		assertResponse(response, 204);
	}

	@Test
	public void testAddMultipleMetricLongValueSuccess() throws Exception
	{
		String json = Resources.toString(Resources.getResource("multi-metric-long.json"), Charsets.UTF_8);

		JsonResponse response = post(json, ADD_METRIC_URL);

		assertThat(response.getStatusCode(), equalTo(204));
	}

	@Test
	public void testAddMissingName() throws Exception
	{
		String json = Resources.toString(Resources.getResource("single-metric-missing-name.json"), Charsets.UTF_8);

		JsonResponse response = post(json, ADD_METRIC_URL);

		assertResponse(response, 400, "{\"errors\":[\"metricsRequest[0].name may not be empty\"]}");
	}

	@Test
	public void testAddInvalidTimestamp() throws Exception
	{
		String json = Resources.toString(Resources.getResource("multi-metric-invalid-timestamp.json"), Charsets.UTF_8);

		JsonResponse response = post(json, ADD_METRIC_URL);

		assertResponse(response, 400, "{\"errors\":[\"timestamp must be greater than or equal to 1\"]}");
	}

	@Test
	public void testQuery() throws IOException
	{
		String json = Resources.toString(Resources.getResource("query-metric-absolute-dates.json"), Charsets.UTF_8);

		JsonResponse response = post(json, GET_METRIC_URL);

		assertResponse(response, 200,
				"{\"queries\":" +
						"[{\"results\":" +
						"[{\"name\":\"abc.123\",\"tags\":{\"server\":[\"server1\",\"server2\"]},\"values\":[[1,60.2],[2,30.200000000000003],[3,20.1]]}]}]}");
	}

	@Test
	public void testQueryWithBeanValidationException() throws IOException
	{
		String json = Resources.toString(Resources.getResource("query-metric-invalid-relative-unit.json"), Charsets.UTF_8);

		JsonResponse response = post(json, GET_METRIC_URL);

		assertResponse(response, 400,
				"{\"errors\":[\"startRelative.unit must be one of SECONDS,MINUTES,HOURS,DAYS,WEEKS,MONTHS,YEARS\"]}");
	}

	@Test
	public void testQueryWithJsonMapperParsingException() throws IOException
	{
		String json = Resources.toString(Resources.getResource("query-metric-invalid-json.json"), Charsets.UTF_8);

		JsonResponse response = post(json, GET_METRIC_URL);

		assertResponse(response, 400,
				"{\"errors\":[\"Invalid json for Java type MetricRequestList:Can not instantiate value of type " +
						"[simple type, class net.opentsdb.core.http.rest.json.QueryRequest] from JSON String; " +
						"no single-String constructor/factory method\"]}");
	}

	@Test
	public void testMetricNames() throws IOException
	{
		JsonResponse response = get(METRIC_NAMES_URL);

		assertResponse(response, 200, "{\"results\":[\"cpu\",\"memory\",\"disk\",\"network\"]}");
	}

	@Test
	public void testTagNames() throws IOException
	{
		JsonResponse response = get(TAG_NAMES_URL);

		assertResponse(response, 200, "{\"results\":[\"server1\",\"server2\",\"server3\"]}");
	}

	@Test
	public void testTagValues() throws IOException
	{
		JsonResponse response = get(TAG_VALUES_URL);

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

	private JsonResponse post(String json, String url) throws IOException
	{
		HttpPost post = new HttpPost(url);
		post.setHeader(CONTENT_TYPE, APPLICATION_JSON);
		post.setEntity(new StringEntity(json));


		HttpResponse response = client.execute(post);
		return new JsonResponse(response);
	}

	private JsonResponse get(String url) throws IOException
	{
		HttpGet get = new HttpGet(url);
		HttpResponse response = client.execute(get);
		return new JsonResponse(response);
	}

	public static class TestDatastore extends Datastore
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
		protected List<DataPointRow> queryDatabase(DatastoreMetricQuery query, CachedSearchResult cachedSearchResult)
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
	}

}