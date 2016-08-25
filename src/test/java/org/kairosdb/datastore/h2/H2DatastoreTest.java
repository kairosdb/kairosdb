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
package org.kairosdb.datastore.h2;


import org.hamcrest.CoreMatchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kairosdb.core.DataPointListener;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.TestDataPointFactory;
import org.kairosdb.core.datastore.*;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.datastore.DatastoreTestHelper;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static junit.framework.Assert.assertFalse;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class H2DatastoreTest extends DatastoreTestHelper
{
	public static final String DB_PATH = "build/h2db_test";


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

		s_datastore = new KairosDatastore(new H2Datastore(DB_PATH, dataPointFactory),
				new QueryQueuingManager(1, "hostname"),
				Collections.<DataPointListener>emptyList(), dataPointFactory, false);

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
	 @throws DatastoreException
	 */
	@Test
	public void test_queryDatabase_noMetric() throws DatastoreException
	{

		Map<String, String> tags = new TreeMap<String, String>();
		QueryMetric query = new QueryMetric(500, 0, "metric_not_there");
		query.setEndTime(3000);

		query.setTags(tags);

		DatastoreQuery dq = super.s_datastore.createQuery(query);

		List<DataPointGroup> results = dq.execute();

		assertThat(results.size(), CoreMatchers.equalTo(1));
		DataPointGroup dpg = results.get(0);
		assertThat(dpg.getName(), is("metric_not_there"));
		assertFalse(dpg.hasNext());

		dq.close();
	}

}
