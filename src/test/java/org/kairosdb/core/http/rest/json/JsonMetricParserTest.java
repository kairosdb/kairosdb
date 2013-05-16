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
import com.google.gson.stream.MalformedJsonException;
import org.junit.Test;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datastore.*;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.http.rest.validation.ValidationException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

public class JsonMetricParserTest
{
	@Test
	public void test_nullMetricName_Invalid() throws DatastoreException, IOException
	{
		String json = "[{\"name\": \"metric1\", \"datapoints\": [[1,2]]}, {\"datapoints\": [[1,2]]}]";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, Collections.EMPTY_LIST);
		JsonMetricParser parser = new JsonMetricParser(datastore, new ByteArrayInputStream(json.getBytes()));

		try
		{
			parser.parse();
			fail("Should throw ValidationException");
		}
		catch (ValidationException e)
		{
			assertThat(e.getMessage(), equalTo("metric[1].name may not be null."));
		}
	}

	@Test
	public void test_timestampButNoValue_Invalid() throws DatastoreException, IOException
	{
		String json = "[{\"name\": \"metric1\", \"timestamp\": 1234}]";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, Collections.EMPTY_LIST);
		JsonMetricParser parser = new JsonMetricParser(datastore, new ByteArrayInputStream(json.getBytes()));

		try
		{
			parser.parse();
			fail("Should throw ValidationException");
		}
		catch (ValidationException e)
		{
			assertThat(e.getMessage(), equalTo("metric[0].value cannot be null or empty."));
		}
	}

	@Test
	public void test_valueButNoTimestamp_Invalid() throws DatastoreException, IOException
	{
		String json = "[{\"name\": \"metric1\", \"value\": 1234}]";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, Collections.EMPTY_LIST);
		JsonMetricParser parser = new JsonMetricParser(datastore, new ByteArrayInputStream(json.getBytes()));

		try
		{
			parser.parse();
			fail("Should throw ValidationException");
		}
		catch (ValidationException e)
		{
			assertThat(e.getMessage(), equalTo("metric[0].timestamp must be greater than 0."));
		}
	}

	@Test
	public void test_emptyMetricName_Invalid() throws DatastoreException, IOException
	{
		String json = "[{\"name\": \"\", \"datapoints\": [[1,2]]}]";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, Collections.EMPTY_LIST);
		JsonMetricParser parser = new JsonMetricParser(datastore, new ByteArrayInputStream(json.getBytes()));

		try
		{
			parser.parse();
			fail("Should throw ValidationException");
		}
		catch (ValidationException e)
		{
			assertThat(e.getMessage(), equalTo("metric[0].name may not be empty."));
		}
	}

	@Test
	public void test_validJsonWithTimestampValue() throws DatastoreException, IOException, ValidationException
	{
		String json = "[{\"name\": \"metric1\", \"timestamp\": 1234, \"value\": 4321}]";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, Collections.EMPTY_LIST);
		JsonMetricParser parser = new JsonMetricParser(datastore, new ByteArrayInputStream(json.getBytes()));

		parser.parse();

		List<DataPointSet> dataPointSetList = fakeds.getDataPointSetList();
		assertThat(dataPointSetList.size(), equalTo(1));

		assertThat(dataPointSetList.get(0).getName(), equalTo("metric1"));
		assertThat(dataPointSetList.get(0).getTags().size(), equalTo(0));
		assertThat(dataPointSetList.get(0).getDataPoints().size(), equalTo(1));
		assertThat(dataPointSetList.get(0).getDataPoints().get(0).getTimestamp(), equalTo(1234L));
		assertThat(dataPointSetList.get(0).getDataPoints().get(0).getLongValue(), equalTo(4321L));
	}

	@Test(expected = MalformedJsonException.class)
	public void test_invaidJson() throws DatastoreException, IOException, ValidationException
	{
		String json = "[{\"name\": \"metric1\", \"timestamp\": 1234, \"value\": }]";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, Collections.EMPTY_LIST);
		JsonMetricParser parser = new JsonMetricParser(datastore, new ByteArrayInputStream(json.getBytes()));

		parser.parse();

		List<DataPointSet> dataPointSetList = fakeds.getDataPointSetList();
		assertThat(dataPointSetList.size(), equalTo(1));

		assertThat(dataPointSetList.get(0).getName(), equalTo("metric1"));
		assertThat(dataPointSetList.get(0).getTags().size(), equalTo(0));
		assertThat(dataPointSetList.get(0).getDataPoints().size(), equalTo(1));
		assertThat(dataPointSetList.get(0).getDataPoints().get(0).getTimestamp(), equalTo(1234L));
		assertThat(dataPointSetList.get(0).getDataPoints().get(0).getLongValue(), equalTo(4321L));
	}

	@Test
	public void test_validJsonWithTimestampValueAndDataPoints() throws DatastoreException, IOException, ValidationException
	{
		String json = "[{\"name\": \"metric1\", \"timestamp\": 1234, \"value\": 4321, \"datapoints\": [[456, 654]]}]";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, Collections.EMPTY_LIST);
		JsonMetricParser parser = new JsonMetricParser(datastore, new ByteArrayInputStream(json.getBytes()));

		parser.parse();

		List<DataPointSet> dataPointSetList = fakeds.getDataPointSetList();
		assertThat(dataPointSetList.size(), equalTo(1));

		assertThat(dataPointSetList.get(0).getName(), equalTo("metric1"));
		assertThat(dataPointSetList.get(0).getTags().size(), equalTo(0));
		assertThat(dataPointSetList.get(0).getDataPoints().size(), equalTo(2));
		assertThat(dataPointSetList.get(0).getDataPoints().get(0).getTimestamp(), equalTo(456L));
		assertThat(dataPointSetList.get(0).getDataPoints().get(0).getLongValue(), equalTo(654L));
		assertThat(dataPointSetList.get(0).getDataPoints().get(1).getTimestamp(), equalTo(1234L));
		assertThat(dataPointSetList.get(0).getDataPoints().get(1).getLongValue(), equalTo(4321L));
	}

	@Test
	public void test_validJsonWithDatapoints() throws DatastoreException, IOException, ValidationException
	{
		String json = Resources.toString(Resources.getResource("json-metric-parser-multiple-metric.json"), Charsets.UTF_8);

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, Collections.EMPTY_LIST);
		JsonMetricParser parser = new JsonMetricParser(datastore, new ByteArrayInputStream(json.getBytes()));

		parser.parse();

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
		public List<DataPointRow> queryDatabase(DatastoreMetricQuery query, CachedSearchResult cachedSearchResult) throws DatastoreException
		{
			return null;
		}

		@Override
		public void deleteDataPoints(DatastoreMetricQuery deleteQuery) throws DatastoreException
		{
			//To change body of implemented methods use File | Settings | File Templates.
		}
	}
}