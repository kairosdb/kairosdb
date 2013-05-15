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


import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.datastore.DatastoreTestHelper;
import org.kairosdb.core.exception.DatastoreException;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;
import java.util.Collections;

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
		s_datastore = new KairosDatastore(new H2Datastore(DB_PATH), Collections.EMPTY_LIST);

		loadData();
	}

	@AfterClass
	public static void cleanupDatabase() throws InterruptedException, DatastoreException
	{
		s_datastore.close();
		File dbDir = new File(DB_PATH);
		deltree(dbDir);
	}


}
