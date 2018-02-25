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

package org.kairosdb.core;

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import org.json.JSONException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.kairosdb.core.aggregator.Sampling;
import org.kairosdb.core.aggregator.SumAggregator;
import org.kairosdb.core.datapoints.DoubleDataPointFactoryImpl;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.DatastoreQuery;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.datastore.TimeUnit;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.util.ValidationException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.Socket;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ExportTest
{
	public static final String METRIC_NAME = "kairos.import_export_unit_test";
	private static Main s_main;
	private static Injector s_injector;
	public static final long LOAD = 1000L;

	@SuppressWarnings("ResultOfMethodCallIgnored")
	@BeforeClass
	public static void setup()
	{
		new File("build").mkdir();
	}

	private static void loadData(int port) throws IOException, InterruptedException
	{
		Socket sock = new Socket("localhost", port);

		long start = System.currentTimeMillis() - (LOAD);
		PrintWriter os = new PrintWriter(sock.getOutputStream());

		for (long i = 0; i < LOAD; i++)
		{
			os.println("putm "+METRIC_NAME+" "+String.valueOf(i+start)+ " 42 host=A");
			os.println("putm "+METRIC_NAME+" "+String.valueOf(i+start)+ " 42 host=B");
			os.println("putm "+METRIC_NAME+" "+String.valueOf(i+start)+ " 42.5 host=C");
		}

		os.close();
		sock.close();
		Thread.sleep(2000);
	}

	@BeforeClass
	public static void loadData() throws IOException, KairosDBException, InterruptedException
	{
		File props = new File("kairosdb.properties");
		if (!props.exists())
			props = null;

		//Ensure the memory queue processor is used
		System.setProperty("kairosdb.queue_processor.class", "org.kairosdb.core.queue.MemoryQueueProcessor");
		s_main = new Main(props);
		s_main.startServices();
		s_injector = s_main.getInjector();

		//make sure it is cleared out
		deleteData();
		//Load data to be exported
		int port = s_injector.getInstance(Key.get(Integer.class, Names.named("kairosdb.telnetserver.port")));
		loadData(port);

	}

	private static void deleteData() throws DatastoreException, InterruptedException
	{
		KairosDatastore ds = s_injector.getInstance(KairosDatastore.class);

		QueryMetric metric = new QueryMetric(Long.MIN_VALUE, Long.MAX_VALUE, 0, METRIC_NAME);
		ds.delete(metric);
		Thread.sleep(500);
	}

	@AfterClass
	public static void cleanup() throws InterruptedException, DatastoreException
	{
		deleteData();

		s_main.stopServices();
	}


	@Test
	public void test1_testExport() throws IOException, DatastoreException, JSONException, ValidationException, InterruptedException
	{
		verifyDataPoints();

		Writer ps = new OutputStreamWriter(new FileOutputStream("build/export.json"), "UTF-8");
		s_main.runExport(ps, Collections.singletonList(METRIC_NAME));
		ps.flush();
		ps.close();
	}


	@Test
	public void test2_testImport() throws InterruptedException, DatastoreException, JSONException, IOException, ValidationException
	{
		deleteData();
		KairosDatastore ds = s_injector.getInstance(KairosDatastore.class);

		//Assert data is not there.
		QueryMetric queryMetric = new QueryMetric(0, 0, METRIC_NAME);
		DatastoreQuery query = ds.createQuery(queryMetric);
		List<DataPointGroup> results = query.execute();
		assertThat(results.size(), equalTo(1));
		assertThat(results.get(0).hasNext(), equalTo(false));

		query.close();

		InputStream export = new FileInputStream("build/export.json");

		s_main.runImport(export);

		export.close();
		Thread.sleep(2000);

		verifyDataPoints();
	}

	private void verifyDataPoints() throws DatastoreException
	{
		KairosDatastore ds = s_injector.getInstance(KairosDatastore.class);

		QueryMetric queryMetric = new QueryMetric(0, 0, METRIC_NAME);
		SumAggregator sum = new SumAggregator(new DoubleDataPointFactoryImpl());
		sum.setSampling(new Sampling(100, TimeUnit.YEARS));
		queryMetric.addAggregator(sum);
		DatastoreQuery query = ds.createQuery(queryMetric);
		List<DataPointGroup> results = query.execute();

		assertThat(results.size(), equalTo(1));
		assertThat(results.get(0).hasNext(), equalTo(true));
		assertThat(results.get(0).next().getDoubleValue(), equalTo(126500.0));

		query.close();
	}
}
