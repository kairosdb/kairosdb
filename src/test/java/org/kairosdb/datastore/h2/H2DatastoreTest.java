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
package org.kairosdb.datastore.h2;


import org.hamcrest.CoreMatchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.TestDataPointFactory;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.DatastoreQuery;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.datastore.QueryQueuingManager;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.datastore.DatastoreTestHelper;
import org.kairosdb.core.datastore.ServiceKeyValue;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertFalse;

public class H2DatastoreTest extends DatastoreTestHelper
{
	private static final String DB_PATH = "build/h2db_test";

	private static H2Datastore h2Datastore;


	@SuppressWarnings({"ResultOfMethodCallIgnored", "ConstantConditions"})
	private static void deltree(File directory)
	{
		if (!directory.exists())
			return;
		File[] list = directory.listFiles();

		for (File file : list)
		{
			if (file.isDirectory())
				deltree(file);

			file.delete();
		}

		directory.delete();
	}



	@BeforeClass
	public static void setupDatabase() throws DatastoreException
	{
		KairosDataPointFactory dataPointFactory = new TestDataPointFactory();
		h2Datastore = new H2Datastore(DB_PATH, dataPointFactory, s_eventBus);

		s_datastore = new KairosDatastore(h2Datastore,
				new QueryQueuingManager(1, "hostname"),
				dataPointFactory, false);

		s_eventBus.register(h2Datastore);

		loadData();
	}

	@AfterClass
	public static void cleanupDatabase() throws InterruptedException, DatastoreException
	{
		s_datastore.close();
		File dbDir = new File(DB_PATH);
		deltree(dbDir);
	}

	/**
	 This is here because hbase throws an exception in this case
	 */
	@Test
	public void test_queryDatabase_noMetric() throws DatastoreException
	{

		Map<String, String> tags = new TreeMap<>();
		QueryMetric query = new QueryMetric(500, 0, "metric_not_there");
		query.setEndTime(3000);

		query.setTags(tags);

		DatastoreQuery dq = s_datastore.createQuery(query);

		List<DataPointGroup> results = dq.execute();

		assertThat(results.size(), CoreMatchers.equalTo(1));
		DataPointGroup dpg = results.get(0);
		assertThat(dpg.getName(), is("metric_not_there"));
		assertFalse(dpg.hasNext());

		dq.close();
	}

	@Test
	public void test_serviceKeyStore_singleService()
			throws DatastoreException
	{
		h2Datastore.setValue("Service", "ServiceKey", "key1", "value1");
		h2Datastore.setValue("Service", "ServiceKey", "key2", "value2");
		h2Datastore.setValue("Service", "ServiceKey", "foo", "value3");

		// Test setValue and getValue
		assertServiceKeyValue("Service", "ServiceKey", "key1", "value1");
		assertServiceKeyValue("Service", "ServiceKey", "key2", "value2");
		assertServiceKeyValue("Service", "ServiceKey", "foo", "value3");

		// Test lastModified value changes
		long lastModified = h2Datastore.getValue("Service", "ServiceKey", "key2").getLastModified().getTime();
		h2Datastore.setValue("Service", "ServiceKey", "key2", "changed");
		assertServiceKeyValue("Service", "ServiceKey", "key2", "changed");
		assertThat(h2Datastore.getValue("Service", "ServiceKey", "key2").getLastModified().getTime(), greaterThan(lastModified));

		// Test listKeys
		assertThat(h2Datastore.listKeys("Service", "ServiceKey"), hasItems("foo", "key1", "key2"));
		assertThat(h2Datastore.listKeys("Service", "ServiceKey", "key"), hasItems("key1", "key2"));

		// Test delete
		lastModified = h2Datastore.getServiceKeyLastModifiedTime("Service", "ServiceKey").getTime();
		h2Datastore.deleteKey("Service", "ServiceKey", "key2");
		assertThat(h2Datastore.listKeys("Service", "ServiceKey"), hasItems("foo", "key1"));
		assertThat(h2Datastore.getValue("Service", "ServiceKey", "key2"), is(nullValue()));
		assertThat(h2Datastore.getServiceKeyLastModifiedTime("Service", "ServiceKey").getTime(), greaterThan(lastModified));

		lastModified = h2Datastore.getServiceKeyLastModifiedTime("Service", "ServiceKey").getTime();
		h2Datastore.deleteKey("Service", "ServiceKey", "foo");
		assertThat(h2Datastore.listKeys("Service", "ServiceKey"), hasItems("key1"));
		assertThat(h2Datastore.getValue("Service", "ServiceKey", "foo"), is(nullValue()));
		assertThat(h2Datastore.getServiceKeyLastModifiedTime("Service", "ServiceKey").getTime(), greaterThan(lastModified));
	}

	@Test
	public void tet_serviceKeyStore_multipleServices()
			throws DatastoreException
	{
		h2Datastore.setValue("Service1", "ServiceKey1", "key1", "value1");
		h2Datastore.setValue("Service1", "ServiceKey2", "key1", "value2");
		h2Datastore.setValue("Service1", "ServiceKey3", "key1", "value3");

		h2Datastore.setValue("Service2", "ServiceKey1", "key1", "value4");
		h2Datastore.setValue("Service2", "ServiceKey1", "key2", "value5");
		h2Datastore.setValue("Service2", "ServiceKey1", "key3", "value6");
		h2Datastore.setValue("Service2", "ServiceKey1", "key4", "value7");

		h2Datastore.setValue("Service3", "ServiceKey1", "foo", "value8");
		h2Datastore.setValue("Service3", "ServiceKey1", "bar", "value9");

		// Test listKeys
		assertThat(h2Datastore.listKeys("Service1", "ServiceKey1"), hasItems("key1"));
		assertThat(h2Datastore.listKeys("Service1", "ServiceKey2"), hasItems("key1"));
		assertThat(h2Datastore.listKeys("Service1", "ServiceKey3"), hasItems("key1"));
		assertThat(h2Datastore.listKeys("Service2", "ServiceKey1"), hasItems("key1", "key2", "key3", "key4"));
		assertThat(h2Datastore.listKeys("Service3", "ServiceKey1"), hasItems("bar", "foo"));

		// Test listServiceKeys
		assertThat(h2Datastore.listServiceKeys("Service1"), hasItems("ServiceKey1", "ServiceKey2", "ServiceKey3"));

		// Test get
		assertServiceKeyValue("Service1", "ServiceKey1", "key1", "value1");
		assertServiceKeyValue("Service1", "ServiceKey2", "key1", "value2");
		assertServiceKeyValue("Service1", "ServiceKey3", "key1", "value3");
		assertServiceKeyValue("Service2", "ServiceKey1", "key1", "value4");
		assertServiceKeyValue("Service2", "ServiceKey1", "key2", "value5");
		assertServiceKeyValue("Service2", "ServiceKey1", "key3", "value6");
		assertServiceKeyValue("Service2", "ServiceKey1", "key4", "value7");
		assertServiceKeyValue("Service3", "ServiceKey1", "foo", "value8");
		assertServiceKeyValue("Service3", "ServiceKey1", "bar", "value9");
	}

	private void assertServiceKeyValue(String service, String serviceKey, String key, String expected)
			throws DatastoreException
	{
		ServiceKeyValue value = h2Datastore.getValue(service, serviceKey, key);
		assertThat(value.getValue(), equalTo(expected));
		assertThat(value.getLastModified(), is(notNullValue()));
	}
}
