// KairosDB2
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
package org.kairosdb.datastore.h2;


import org.kairosdb.datastore.DatastoreTestHelper;
import org.kairosdb.core.exception.DatastoreException;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;

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
		s_datastore = new H2Datastore(DB_PATH);

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
