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
package net.opentsdb.datastore.cassandra;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPointSet;
import net.opentsdb.core.datastore.DataPointGroup;
import net.opentsdb.core.datastore.DatastoreMetricQuery;
import net.opentsdb.core.datastore.QueryMetric;
import net.opentsdb.core.exception.DatastoreException;
import net.opentsdb.datastore.DatastoreMetricQueryImpl;
import net.opentsdb.datastore.DatastoreTestHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 1/25/13
 Time: 12:53 PM
 To change this template use File | Settings | File Templates.
 */
public class CassandraDatastoreTest extends DatastoreTestHelper
{
	public static final String ROW_KEY_TEST_METRIC = "row_key_test_metric";
	public static final String ROW_KEY_BIG_METRIC = "row_key_big_metric";

	private static final int MAX_ROW_READ_SIZE=1024;
	private static final int OVERFLOW_SIZE = MAX_ROW_READ_SIZE * 2 + 10;

	private static CassandraDatastore s_datastore;
	private static long s_dataPointTime;

	private static void loadCassandraData()
	{
		s_dataPointTime = System.currentTimeMillis();

		DataPointSet dpSet = new DataPointSet(ROW_KEY_TEST_METRIC);
		dpSet.addTag("host", "A");
		dpSet.addTag("client", "foo");

		dpSet.addDataPoint(new DataPoint(s_dataPointTime, 42));

		s_datastore.putDataPoints(dpSet);


		dpSet = new DataPointSet(ROW_KEY_TEST_METRIC);
		dpSet.addTag("host", "B");
		dpSet.addTag("client", "foo");

		dpSet.addDataPoint(new DataPoint(s_dataPointTime, 42));

		s_datastore.putDataPoints(dpSet);


		dpSet = new DataPointSet(ROW_KEY_TEST_METRIC);
		dpSet.addTag("host", "C");
		dpSet.addTag("client", "bar");

		dpSet.addDataPoint(new DataPoint(s_dataPointTime, 42));

		s_datastore.putDataPoints(dpSet);


		dpSet = new DataPointSet(ROW_KEY_TEST_METRIC);
		dpSet.addTag("host", "D");
		dpSet.addTag("client", "bar");

		dpSet.addDataPoint(new DataPoint(s_dataPointTime, 42));

		s_datastore.putDataPoints(dpSet);


		// Add a row of data that is larger than MAX_ROW_READ_SIZE
		dpSet = new DataPointSet(ROW_KEY_BIG_METRIC);
		dpSet.addTag("host", "E");

		for (int i = OVERFLOW_SIZE; i > 0; i--)
		{
			dpSet.addDataPoint(new DataPoint(s_dataPointTime - (long)i, 42));
		}

		s_datastore.putDataPoints(dpSet);
	}

	@BeforeClass
	public static void setupDatastore() throws InterruptedException, DatastoreException
	{
		s_datastore = new CassandraDatastore("localhost",
				"9160", 1, 7257600000L, MAX_ROW_READ_SIZE, MAX_ROW_READ_SIZE);

		DatastoreTestHelper.s_datastore = s_datastore;

		loadCassandraData();
		loadData();
		Thread.sleep(2000);

	}

	@AfterClass
	public static void closeDatastore() throws InterruptedException
	{
		s_datastore.close();
	}

	@Test
	public void test_getKeysForQuery()
	{
		DatastoreMetricQuery query = new DatastoreMetricQueryImpl(ROW_KEY_TEST_METRIC,
				Collections.<String, String>emptyMap(), s_dataPointTime, s_dataPointTime);

		List<DataPointsRowKey> keys = s_datastore.getKeysForQuery(query);

		assertEquals(4, keys.size());
	}

	@Test
	public void test_getKeysForQuery_withFilter()
	{
		Map<String, String> tagFilter = new HashMap<String, String>();
		tagFilter.put("client", "bar");

		DatastoreMetricQuery query = new DatastoreMetricQueryImpl(ROW_KEY_TEST_METRIC,
				tagFilter, s_dataPointTime, s_dataPointTime);

		List<DataPointsRowKey> keys = s_datastore.getKeysForQuery(query);

		assertEquals(2, keys.size());
	}

	@Test
	public void test_rowLargerThanMaxReadSize() throws DatastoreException
	{
		Map<String, String> tagFilter = new HashMap<String, String>();
		tagFilter.put("host", "E");

		QueryMetric query = new QueryMetric(s_dataPointTime - OVERFLOW_SIZE, 0, ROW_KEY_BIG_METRIC, "none");
		query.setEndTime(s_dataPointTime);
		query.setTags(tagFilter);

		List<DataPointGroup> results = s_datastore.query(query);

		DataPointGroup dataPointGroup = results.get(0);
		int counter = 0;
		while(dataPointGroup.hasNext())
		{
			dataPointGroup.next();
			counter++;
		}

		assertEquals(OVERFLOW_SIZE, counter);
	}

}
