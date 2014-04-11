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
package org.kairosdb.core.http.rest.json;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.junit.Test;
import org.kairosdb.core.DataPointListener;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datastore.*;
import org.kairosdb.core.exception.DatastoreException;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;

@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
public class JsonMetricParserTest
{
	@Test
	public void test_emptyJson_Invalid() throws DatastoreException, IOException
	{
		String json = "";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, new QueryQueuingManager(1, "hostname"),
				Collections.<DataPointListener>emptyList(), "hostname");

		JsonMetricParser parser = new JsonMetricParser(datastore, new StringReader(json), new Gson());

		ValidationErrors validationErrors = parser.parse();

		assertThat(validationErrors.size(), equalTo(1));
		assertThat(validationErrors.getFirstError(), equalTo("Invalid json. No content due to end of input."));
	}

	@Test
	public void test_nullMetricName_Invalid() throws DatastoreException, IOException
	{
		String json = "[{\"name\": \"metric1\", \"timestamp\": 1234, \"value\": 456, \"datapoints\": [[1,2]], \"tags\":{\"foo\":\"bar\"}}, {\"datapoints\": [[1,2]], \"tags\":{\"foo\":\"bar\"}}]";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, new QueryQueuingManager(1, "hostname"),
				Collections.<DataPointListener>emptyList(), "hostname");

		JsonMetricParser parser = new JsonMetricParser(datastore, new StringReader(json), new Gson());

		ValidationErrors validationErrors = parser.parse();

		assertThat(validationErrors.size(), equalTo(1));
		assertThat(validationErrors.getFirstError(), equalTo("metric[1].name may not be null."));
	}

	@Test
	public void test_timestampButNoValue_Invalid() throws DatastoreException, IOException
	{
		String json = "[{\"name\": \"metric1\", \"timestamp\": 1234, \"tags\": {\"foo\":\"bar\"}}]";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, new QueryQueuingManager(1, "hostname"),
				Collections.<DataPointListener>emptyList(), "hostname");
		JsonMetricParser parser = new JsonMetricParser(datastore, new StringReader(json), new Gson());

		ValidationErrors validationErrors = parser.parse();

		assertThat(validationErrors.size(), equalTo(1));
		assertThat(validationErrors.getFirstError(), equalTo("metric[0](name=metric1).value may not be null."));
	}

	@Test
	public void test_valueButNoTimestamp_Invalid() throws DatastoreException, IOException
	{
		String json = "[{\"name\": \"metric1\", \"value\": 1234, \"tags\":{\"foo\":\"bar\"}}]";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, new QueryQueuingManager(1, "hostname"),
				Collections.<DataPointListener>emptyList(), "hostname");
		JsonMetricParser parser = new JsonMetricParser(datastore, new StringReader(json), new Gson());

		ValidationErrors validationErrors = parser.parse();

		assertThat(validationErrors.size(), equalTo(1));
		assertThat(validationErrors.getFirstError(), equalTo("metric[0](name=metric1).timestamp must be greater than or equal to 1."));
	}

	@Test
	public void test_datapoints_empty_Invalid() throws DatastoreException, IOException
	{
		String json = "[{\"name\": \"metric1\", \"tags\":{\"foo\":\"bar\"}, \"datapoints\": [[]]}]";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, new QueryQueuingManager(1, "hostname"),
				Collections.<DataPointListener>emptyList(), "hostname");
		JsonMetricParser parser = new JsonMetricParser(datastore, new StringReader(json), new Gson());

		ValidationErrors validationErrors = parser.parse();

		assertThat(validationErrors.size(), equalTo(1));
		assertThat(validationErrors.getFirstError(), equalTo("metric[0](name=metric1).datapoints[0].timestamp cannot be null or empty."));
	}

	@Test(expected = JsonSyntaxException.class)
	public void test_datapoints_empty_value_Invalid() throws DatastoreException, IOException
	{
		String json = "[{\"name\": \"metric1\", \"tags\":{\"foo\":\"bar\"}, \"datapoints\": [[2,]]}]";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, new QueryQueuingManager(1, "hostname"),
				Collections.<DataPointListener>emptyList(), "hostname");
		JsonMetricParser parser = new JsonMetricParser(datastore, new StringReader(json), new Gson());

		ValidationErrors validationErrors = parser.parse();

		assertThat(validationErrors.size(), equalTo(1));
		assertThat(validationErrors.getFirstError(), equalTo("metric[0].datapoints[0].value cannot be null or empty."));
	}

	@Test(expected = JsonSyntaxException.class)
	public void test_datapoints_empty_timestamp_Invalid() throws DatastoreException, IOException
	{
		String json = "[{\"name\": \"metric1\", \"tags\":{\"foo\":\"bar\"}, \"datapoints\": [[,2]]}]";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, new QueryQueuingManager(1, "hostname"),
				Collections.<DataPointListener>emptyList(), "hostname");
		JsonMetricParser parser = new JsonMetricParser(datastore, new StringReader(json), new Gson());

		ValidationErrors validationErrors = parser.parse();

		assertThat(validationErrors.size(), equalTo(1));
		assertThat(validationErrors.getFirstError(), equalTo("metric[0].datapoints[0].value cannot be null or empty."));
	}

	@Test
	public void test_datapoints_timestamp_Invalid() throws DatastoreException, IOException
	{
		String json = "[{\"name\": \"metric1\", \"tags\":{\"foo\":\"bar\"}, \"datapoints\": [[0, 1234]]}]";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, new QueryQueuingManager(1, "hostname"),
				Collections.<DataPointListener>emptyList(), "hostname");
		JsonMetricParser parser = new JsonMetricParser(datastore, new StringReader(json), new Gson());

		ValidationErrors validationErrors = parser.parse();

		assertThat(validationErrors.size(), equalTo(1));
		assertThat(validationErrors.getFirstError(), equalTo("metric[0](name=metric1).datapoints[0].value cannot be null or empty. must be greater than or equal to 1."));
	}

	@Test
	public void test_emptyMetricName_Invalid() throws DatastoreException, IOException
	{
		String json = "[{\"name\": \"\", \"tags\":{\"foo\":\"bar\"}, \"datapoints\": [[1,2]]}]";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, new QueryQueuingManager(1, "hostname"),
				Collections.<DataPointListener>emptyList(), "hostname");
		JsonMetricParser parser = new JsonMetricParser(datastore, new StringReader(json), new Gson());

		ValidationErrors validationErrors = parser.parse();

		assertThat(validationErrors.size(), equalTo(1));
		assertThat(validationErrors.getFirstError(), equalTo("metric[0].name may not be empty."));
	}

	@Test
	public void test_metricName_invalidCharacters() throws DatastoreException, IOException
	{
		String json = "[{\"name\": \"bad:name\", \"tags\":{\"foo\":\"bar\"}, \"datapoints\": [[1,2]]}]";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, new QueryQueuingManager(1, "hostname"),
				Collections.<DataPointListener>emptyList(), "hostname");
		JsonMetricParser parser = new JsonMetricParser(datastore, new StringReader(json), new Gson());

		ValidationErrors validationErrors = parser.parse();

		assertThat(validationErrors.size(), equalTo(1));
		assertThat(validationErrors.getFirstError(),
				equalTo("metric[0](name=bad:name) may only contain alphanumeric characters plus periods '.', slash '/', dash '-', and underscore '_'."));
	}

	@Test
	public void test_emptyTags_Invalid() throws DatastoreException, IOException
	{
		String json = "[{\"name\": \"metricName\", \"timestamp\": 12345, \"value\": 456, \"datapoints\": [[1,2]]}]";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, new QueryQueuingManager(1, "hostname"),
				Collections.<DataPointListener>emptyList(), "hostname");
		JsonMetricParser parser = new JsonMetricParser(datastore, new StringReader(json), new Gson());

		ValidationErrors validationErrors = parser.parse();

		assertThat(validationErrors.size(), equalTo(1));
		assertThat(validationErrors.getFirstError(), equalTo("metric[0](name=metricName).tags count must be greater than or equal to 1."));
	}

	@Test
	public void test_emptyTagName_Invalid() throws DatastoreException, IOException
	{
		String json = "[{\"name\": \"metricName\", \"tags\":{\"\":\"bar\"}, \"datapoints\": [[1,2]]}]";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, new QueryQueuingManager(1, "hostname"),
				Collections.<DataPointListener>emptyList(), "hostname");
		JsonMetricParser parser = new JsonMetricParser(datastore, new StringReader(json), new Gson());

		ValidationErrors validationErrors = parser.parse();

		assertThat(validationErrors.size(), equalTo(1));
		assertThat(validationErrors.getFirstError(), equalTo("metric[0](name=metricName).tag[0].name may not be empty."));
	}

	@Test
	public void test_tagName_invalidCharacters() throws DatastoreException, IOException
	{
		String json = "[{\"name\": \"metricName\", \"tags\":{\"bad:name\":\"bar\"}, \"datapoints\": [[1,2]]}]";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, new QueryQueuingManager(1, "hostname"),
				Collections.<DataPointListener>emptyList(), "hostname");
		JsonMetricParser parser = new JsonMetricParser(datastore, new StringReader(json), new Gson());

		ValidationErrors validationErrors = parser.parse();

		assertThat(validationErrors.size(), equalTo(1));
		assertThat(validationErrors.getFirstError(),
				equalTo("metric[0](name=metricName).tag[bad:name] may only contain alphanumeric characters plus periods '.', slash '/', dash '-', and underscore '_'."));
	}

	@Test
	public void test_emptyTagValue_Invalid() throws DatastoreException, IOException
	{
		String json = "[{\"name\": \"metricName\", \"tags\":{\"foo\":\"\"}, \"datapoints\": [[1,2]]}]";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, new QueryQueuingManager(1, "hostname"),
				Collections.<DataPointListener>emptyList(), "hostname");
		JsonMetricParser parser = new JsonMetricParser(datastore, new StringReader(json), new Gson());

		ValidationErrors validationErrors = parser.parse();

		assertThat(validationErrors.size(), equalTo(1));
		assertThat(validationErrors.getFirstError(), equalTo("metric[0](name=metricName).tag[foo].value may not be empty."));
	}

	@Test
	public void test_tagValue_invalidCharacters() throws DatastoreException, IOException
	{
		String json = "[{\"name\": \"metricName\", \"tags\":{\"foo\":\"bad:value\"}, \"datapoints\": [[1,2]]}]";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, new QueryQueuingManager(1, "hostname"),
				Collections.<DataPointListener>emptyList(), "hostname");
		JsonMetricParser parser = new JsonMetricParser(datastore, new StringReader(json), new Gson());

		ValidationErrors validationErrors = parser.parse();

		assertThat(validationErrors.size(), equalTo(1));
		assertThat(validationErrors.getFirstError(),
				equalTo("metric[0](name=metricName).tag[foo].value may only contain alphanumeric characters plus periods '.', slash '/', dash '-', and underscore '_'."));
	}

	@Test
	public void test_multipleValidationFailures() throws DatastoreException, IOException
	{
		String json = "[{\"name\": \"metricName\", \"timestamp\": 456, \"value\":\"\", \"tags\":{\"name\":\"\"}}]";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, new QueryQueuingManager(1, "hostname"),
				Collections.<DataPointListener>emptyList(), "hostname");
		JsonMetricParser parser = new JsonMetricParser(datastore, new StringReader(json), new Gson());

		ValidationErrors validationErrors = parser.parse();

		assertThat(validationErrors.size(), equalTo(2));
		assertThat(validationErrors.getErrors(), hasItem("metric[0](name=metricName).tag[name].value may not be empty."));
		assertThat(validationErrors.getErrors(), hasItem("metric[0](name=metricName).value may not be empty."));
	}

	@Test
	public void test_validJsonWithTimestampValue() throws DatastoreException, IOException
	{
		String json = "[{\"name\": \"metric1\", \"timestamp\": 1234, \"value\": 4321, \"tags\":{\"foo\":\"bar\"}}]";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, new QueryQueuingManager(1, "hostname"),
				Collections.<DataPointListener>emptyList(), "hostname");
		JsonMetricParser parser = new JsonMetricParser(datastore, new StringReader(json), new Gson());

		ValidationErrors validationErrors = parser.parse();

		assertThat(validationErrors.hasErrors(), equalTo(false));

		List<DataPointSet> dataPointSetList = fakeds.getDataPointSetList();
		assertThat(dataPointSetList.size(), equalTo(1));

		assertThat(dataPointSetList.get(0).getName(), equalTo("metric1"));
		assertThat(dataPointSetList.get(0).getTags().size(), equalTo(1));
		assertThat(dataPointSetList.get(0).getTags().get("foo"), equalTo("bar"));
		assertThat(dataPointSetList.get(0).getDataPoints().size(), equalTo(1));
		assertThat(dataPointSetList.get(0).getDataPoints().get(0).getTimestamp(), equalTo(1234L));
		assertThat(dataPointSetList.get(0).getDataPoints().get(0).getLongValue(), equalTo(4321L));
	}

	@Test(expected = JsonSyntaxException.class)
	public void test_invalidJson() throws DatastoreException, IOException
	{
		String json = "[{\"name\": \"metric1\", \"timestamp\": 1234, \"value\": }]";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, new QueryQueuingManager(1, "hostname"),
				Collections.<DataPointListener>emptyList(), "hostname");
		JsonMetricParser parser = new JsonMetricParser(datastore, new StringReader(json), new Gson());

		ValidationErrors validationErrors = parser.parse();

		assertThat(validationErrors.hasErrors(), equalTo(false));

		List<DataPointSet> dataPointSetList = fakeds.getDataPointSetList();
		assertThat(dataPointSetList.size(), equalTo(1));

		assertThat(dataPointSetList.get(0).getName(), equalTo("metric1"));
		assertThat(dataPointSetList.get(0).getTags().size(), equalTo(0));
		assertThat(dataPointSetList.get(0).getDataPoints().size(), equalTo(1));
		assertThat(dataPointSetList.get(0).getDataPoints().get(0).getTimestamp(), equalTo(1234L));
		assertThat(dataPointSetList.get(0).getDataPoints().get(0).getLongValue(), equalTo(4321L));
	}

	@Test
	public void test_validJsonWithTimestampValueAndDataPoints() throws DatastoreException, IOException
	{
		String json = "[{\"name\": \"metric1\", \"time\": 1234, \"value\": 4321, \"datapoints\": [[456, 654]], \"tags\":{\"foo\":\"bar\"}}]";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, new QueryQueuingManager(1, "hostname"),
				Collections.<DataPointListener>emptyList(), "hostname");
		JsonMetricParser parser = new JsonMetricParser(datastore, new StringReader(json), new Gson());

		ValidationErrors validationErrors = parser.parse();

		assertThat(validationErrors.hasErrors(), equalTo(false));

		List<DataPointSet> dataPointSetList = fakeds.getDataPointSetList();
		assertThat(dataPointSetList.size(), equalTo(1));

		assertThat(dataPointSetList.get(0).getName(), equalTo("metric1"));
		assertThat(dataPointSetList.get(0).getTags().size(), equalTo(1));
		assertThat(dataPointSetList.get(0).getTags().get("foo"), equalTo("bar"));
		assertThat(dataPointSetList.get(0).getDataPoints().size(), equalTo(2));
		assertThat(dataPointSetList.get(0).getDataPoints().get(0).getTimestamp(), equalTo(1234L));
		assertThat(dataPointSetList.get(0).getDataPoints().get(0).getLongValue(), equalTo(4321L));
		assertThat(dataPointSetList.get(0).getDataPoints().get(1).getTimestamp(), equalTo(456L));
		assertThat(dataPointSetList.get(0).getDataPoints().get(1).getLongValue(), equalTo(654L));
	}

	@Test
	public void test_validJsonWithDatapoints() throws DatastoreException, IOException
	{
		String json = Resources.toString(Resources.getResource("json-metric-parser-multiple-metric.json"), Charsets.UTF_8);

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, new QueryQueuingManager(1, "hostname"),
				Collections.<DataPointListener>emptyList(), "hostname");
		JsonMetricParser parser = new JsonMetricParser(datastore, new StringReader(json), new Gson());

		ValidationErrors validationErrors = parser.parse();

		assertThat(validationErrors.hasErrors(), equalTo(false));

		List<DataPointSet> dataPointSetList = fakeds.getDataPointSetList();
		assertThat(dataPointSetList.size(), equalTo(2));

		assertThat(dataPointSetList.get(0).getName(), equalTo("archive_file_tracked"));
		assertThat(dataPointSetList.get(0).getTags().size(), equalTo(1));
		assertThat(dataPointSetList.get(0).getTags().get("host"), equalTo("server1"));
		assertThat(dataPointSetList.get(0).getDataPoints().size(), equalTo(3));
		assertThat(dataPointSetList.get(0).getDataPoints().get(0).getTimestamp(), equalTo(1349109376L));
		assertThat(dataPointSetList.get(0).getDataPoints().get(0).getLongValue(), equalTo(123L));
		assertThat(dataPointSetList.get(0).getDataPoints().get(1).getTimestamp(), equalTo(1349109377L));
		assertThat(dataPointSetList.get(0).getDataPoints().get(1).getDoubleValue(), equalTo(13.2));
		assertThat(dataPointSetList.get(0).getDataPoints().get(2).getTimestamp(), equalTo(1349109378L));
		assertThat(dataPointSetList.get(0).getDataPoints().get(2).getDoubleValue(), equalTo(23.1));

		assertThat(dataPointSetList.get(1).getName(), equalTo("archive_file_search"));
		assertThat(dataPointSetList.get(1).getTags().size(), equalTo(2));
		assertThat(dataPointSetList.get(1).getTags().get("host"), equalTo("server2"));
		assertThat(dataPointSetList.get(1).getTags().get("customer"), equalTo("Acme"));
		assertThat(dataPointSetList.get(1).getDataPoints().size(), equalTo(1));
		assertThat(dataPointSetList.get(1).getDataPoints().get(0).getTimestamp(), equalTo(1349109378L));
		assertThat(dataPointSetList.get(1).getDataPoints().get(0).getLongValue(), equalTo(321L));
	}

	@Test
	public void test_justObjectNoArray_valid() throws DatastoreException, IOException
	{
		String json = "{\"name\": \"metric1\", \"timestamp\": 1234, \"value\": 4321, \"tags\":{\"foo\":\"bar\"}}";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, new QueryQueuingManager(1, "hostname"),
				Collections.<DataPointListener>emptyList(), "hostname");
		JsonMetricParser parser = new JsonMetricParser(datastore, new StringReader(json), new Gson());

		ValidationErrors validationErrors = parser.parse();

		assertThat(validationErrors.hasErrors(), equalTo(false));

		List<DataPointSet> dataPointSetList = fakeds.getDataPointSetList();
		assertThat(dataPointSetList.size(), equalTo(1));

		assertThat(dataPointSetList.get(0).getName(), equalTo("metric1"));
		assertThat(dataPointSetList.get(0).getTags().size(), equalTo(1));
		assertThat(dataPointSetList.get(0).getTags().get("foo"), equalTo("bar"));
		assertThat(dataPointSetList.get(0).getDataPoints().size(), equalTo(1));
		assertThat(dataPointSetList.get(0).getDataPoints().get(0).getTimestamp(), equalTo(1234L));
		assertThat(dataPointSetList.get(0).getDataPoints().get(0).getLongValue(), equalTo(4321L));
	}

//	@Test
//	public void test_parserSpeed() throws DatastoreException, IOException
//	{
//		Reader skipReader = new InputStreamReader(
//				new GZIPInputStream(ClassLoader.getSystemResourceAsStream("large_import_skip.gz")));
//
//		Reader reader = new InputStreamReader(
//				new GZIPInputStream(ClassLoader.getSystemResourceAsStream("large_import.gz")));
//
//		FakeDataStore fakeds = new FakeDataStore();
//		KairosDatastore datastore = new KairosDatastore(fakeds, new QueryQueuingManager(1, "hostname"),
//				Collections.<DataPointListener>emptyList(), "hostname");
//
//		JsonMetricParser parser = new JsonMetricParser(datastore, skipReader, new Gson());
//		ValidationErrors validationErrors = parser.parse();
//
//		System.out.println(parser.getDataPointCount());
//		System.out.println("No Validation");
//		System.out.println(parser.getIngestTime());
//
//		parser = new JsonMetricParser(datastore, reader, new Gson());
//		validationErrors = parser.parse();
//		System.out.println("With Validation");
//		System.out.println(parser.getIngestTime());
//	}

	private static class FakeDataStore implements Datastore
	{
		List<DataPointSet> dataPointSetList = new ArrayList<DataPointSet>();

		protected FakeDataStore() throws DatastoreException
		{
		}

		public List<DataPointSet> getDataPointSetList()
		{
			return dataPointSetList;
		}

		@Override
		public void close() throws InterruptedException, DatastoreException
		{
		}

		@Override
		public void putDataPoints(DataPointSet dps) throws DatastoreException
		{
			dataPointSetList.add(dps);
		}

		@Override
		public Iterable<String> getMetricNames() throws DatastoreException
		{
			return null;
		}

		@Override
		public Iterable<String> getTagNames() throws DatastoreException
		{
			return null;
		}

		@Override
		public Iterable<String> getTagValues() throws DatastoreException
		{
			return null;
		}

		@Override
		public void queryDatabase(DatastoreMetricQuery query, QueryCallback queryCallback) throws DatastoreException
		{

		}

		@Override
		public void deleteDataPoints(DatastoreMetricQuery deleteQuery) throws DatastoreException
		{
		}

		@Override
		public TagSet queryMetricTags(DatastoreMetricQuery query) throws DatastoreException
		{
			return null;
		}
	}
}