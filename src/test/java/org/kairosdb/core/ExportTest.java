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

import org.kairosdb.core.exception.DatastoreException;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

public class ExportTest
{
	@Test
	public void testExport() throws IOException, DatastoreException
	{
		File props = new File("kairosdb.properties");
		if (!props.exists())
			props = null;

		Main main = new Main(props);

		PrintStream ps = new PrintStream(new FileOutputStream("build/export.json"));
		main.runExport(ps);
		ps.close();
	}
}
