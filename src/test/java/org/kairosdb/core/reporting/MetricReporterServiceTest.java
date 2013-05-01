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
package org.kairosdb.core.reporting;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datastore.CachedSearchResult;
import org.kairosdb.core.datastore.DataPointRow;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.datastore.DatastoreMetricQuery;
import org.kairosdb.core.exception.DatastoreException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.kairosdb.core.reporting.KairosMetricRegistry.Tag;

public class MetricReporterServiceTest
{
	private KairosMetricRegistry registry;
	private MetricReporterService service;
	private TestDatastore datastore;

	@Before
	public void setup() throws DatastoreException
	{
		registry = new KairosMetricRegistry();
		datastore = new TestDatastore();

		service = new MetricReporterService(datastore, registry, 1, "milliseconds", "hostname");
	}

	@Test
	public void test_counter()
	{
		Counter counter = registry.newCounter(new MetricName("foo", "bar", "fum"), new Tag("tag1", "value1"));
		counter.inc();
		counter.inc();
		counter.inc();

		service.run();

		List<DataPoint> dataPoints = datastore.getDataPoints("foo.bar.fum");
		assertThat(dataPoints.size(), equalTo(1));
		assertThat(dataPoints.get(0).getLongValue(), equalTo(3L));

		counter.inc();
		service.run();

		dataPoints = datastore.getDataPoints("foo.bar.fum");
		assertThat(dataPoints.size(), equalTo(1));
		assertThat(dataPoints.get(0).getLongValue(), equalTo(4L));
	}

	private class TestDatastore extends Datastore
	{
		Map<String, List<DataPoint>> dataPoints = new HashMap<String, List<DataPoint>>();

		protected TestDatastore() throws DatastoreException
		{
		}

		/**
		 * Close the datastore
		 */
		@Override
		public void close() throws InterruptedException, DatastoreException
		{
		}

		@Override
		public void putDataPoints(DataPointSet dps) throws DatastoreException
		{
			dataPoints.put(dps.getName(), dps.getDataPoints());
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
		protected List<DataPointRow> queryDatabase(DatastoreMetricQuery query, CachedSearchResult cachedSearchResult) throws DatastoreException
		{
			return null;
		}

		public List<DataPoint> getDataPoints(String name)
		{
			return dataPoints.get(name);
		}
	}
}