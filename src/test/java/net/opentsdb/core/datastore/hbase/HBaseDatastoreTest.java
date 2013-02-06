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

package net.opentsdb.core.datastore.hbase;


import net.opentsdb.core.exception.DatastoreException;
import net.opentsdb.datastore.DatastoreTestHelper;
import net.opentsdb.datastore.hbase.HBaseDatastore;
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
