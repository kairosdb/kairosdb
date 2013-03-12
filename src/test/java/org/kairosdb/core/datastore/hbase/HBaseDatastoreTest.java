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

package org.kairosdb.core.datastore.hbase;


import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.datastore.DatastoreTestHelper;
import org.kairosdb.datastore.hbase.HBaseDatastore;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

interface HBaseTests{}
interface IntegrationTests{}

@Category({HBaseTests.class, IntegrationTests.class})
public class HBaseDatastoreTest extends DatastoreTestHelper
{
	@BeforeClass
	public static void setupDatabase() throws DatastoreException
	{
		s_datastore = new HBaseDatastore("tsdb", "tsdb-uid", "localhost", "", true);

		loadData();

		try
		{
			Thread.sleep(2000);
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
	}

	@AfterClass
	public static void cleanupDatabase() throws InterruptedException, DatastoreException
	{
		s_datastore.close();
	}


}
