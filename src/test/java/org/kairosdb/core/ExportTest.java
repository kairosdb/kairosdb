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

package org.kairosdb.core;

import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.kairosdb.core.exception.DatastoreException;

import java.io.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ExportTest
{
	@BeforeClass
	public static void loadData()
	{

	}


	@Test
	public void test1_testExport() throws IOException, DatastoreException
	{
		System.out.println("Running Export");
		File props = new File("kairosdb.properties");
		if (!props.exists())
			props = null;

		Main main = new Main(props);

		Writer ps = new OutputStreamWriter(new FileOutputStream("build/export.json"), "UTF-8");
		main.runExport(ps, null);
	}

	@Test
	public void test2_testImport()
	{
		System.out.println("Running Import");
	}
}
