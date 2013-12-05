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

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;
import com.google.gson.Gson;
import com.google.inject.*;
import jcmdline.*;
import org.apache.commons.io.FileUtils;
import org.json.JSONException;
import org.json.JSONWriter;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.datastore.QueryCallback;
import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.core.http.rest.json.JsonMetricParser;
import org.kairosdb.core.http.rest.json.ValidationErrors;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.*;
import java.lang.reflect.Constructor;
import java.nio.charset.Charset;
import java.util.*;

public class Main
{
	public static final Logger logger = (Logger) LoggerFactory.getLogger(Main.class);

	public static final Charset UTF_8 = Charset.forName("UTF-8");
	public static final String SERVICE_PREFIX = "kairosdb.service";

	private static FileParam s_propertiesFile = new FileParam("p",
			"a custom properties file", FileParam.IS_FILE & FileParam.IS_READABLE,
			FileParam.OPTIONAL);

	private static FileParam s_exportFile = new FileParam("f",
			"file to save export to or read from depending on command", FileParam.NO_ATTRIBUTES,
			FileParam.OPTIONAL);

	private static StringParam s_exportMetricNames = new StringParam("n",
			"name of metrics to export. If not specified, then all metrics are exported", 1,
			StringParam.UNSPECIFIED_LENGTH, true, true);

	private static StringParam s_exportRecoveryFile = new StringParam("r",
			"full path to a recovery file. The file tracks metrics that have been exported. If export fails and is run " +
					"again it uses this file to pickup where it left off.", 1,
			StringParam.UNSPECIFIED_LENGTH, true, false);

	private static BooleanParam s_appendToExportFile = new BooleanParam("a",
			"Appends to the export file. By default, the export file is overwritten.");

	/**
	 * start is identical to run except that logging data only goes to the log file
	 * and not to standard out as well
	 */
	private static StringParam s_operationCommand = new StringParam("c",
			"command to run: export, import, run, start", new String[]{"run", "start", "export", "import"}, false);

	private final static Object s_shutdownObject = new Object();

	private Injector m_injector;
	private List<KairosDBService> m_services = new ArrayList<KairosDBService>();

	private void loadPlugins(Properties props, final File propertiesFile) throws IOException
	{
		File propDir = propertiesFile.getParentFile();
		if (propDir == null)
			propDir = new File(".");

		String[] pluginProps = propDir.list(new FilenameFilter()
		{
			@Override
			public boolean accept(File dir, String name)
			{
				return (name.endsWith(".properties") && !name.equals(propertiesFile.getName()));
			}
		});

		ClassLoader cl = getClass().getClassLoader();

		for (String prop : pluginProps)
		{
			logger.info("Loading plugin properties: {}", prop);
			//Load the properties file from a jar if there is one first.
			//This way defaults can be set
			InputStream propStream = cl.getResourceAsStream(prop);

			if (propStream != null)
			{
				props.load(propStream);
				propStream.close();
			}

			//Load the file in
			FileInputStream fis = new FileInputStream(new File(propDir, prop));
			props.load(fis);
			fis.close();
		}
	}


	public Main(File propertiesFile) throws IOException
	{
		Properties props = new Properties();
		InputStream is = getClass().getClassLoader().getResourceAsStream("kairosdb.properties");
		props.load(is);
		is.close();

		if (propertiesFile != null)
		{
			FileInputStream fis = new FileInputStream(propertiesFile);
			props.load(fis);
			fis.close();

			loadPlugins(props, propertiesFile);
		}

		List<Module> moduleList = new ArrayList<Module>();
		moduleList.add(new CoreModule(props));

		for (String propName : props.stringPropertyNames())
		{
			if (propName.startsWith(SERVICE_PREFIX))
			{
				Class<?> aClass;
				try
				{
					if ("".equals(props.getProperty(propName)))
						continue;

					aClass = Class.forName(props.getProperty(propName));
					if (Module.class.isAssignableFrom(aClass))
					{
						Constructor<?> constructor = null;

						try
						{
							constructor = aClass.getConstructor(Properties.class);
						}
						catch (NoSuchMethodException ignore)
						{
						}

						/*
						Check if they have a constructor that takes the properties
						if not construct using the default constructor
						 */
						Module mod;
						if (constructor != null)
							mod = (Module) constructor.newInstance(props);
						else
							mod = (Module) aClass.newInstance();

						moduleList.add(mod);
					}
				}
				catch (Exception e)
				{
					logger.error("Unable to load service " + propName, e);
				}
			}
		}

		m_injector = Guice.createInjector(moduleList);
	}


	public static void main(String[] args) throws Exception
	{
		CmdLineHandler cl = new VersionCmdLineHandler("Version 1.0",
				new HelpCmdLineHandler("KairosDB Help", "kairosdb", "Starts KairosDB",
						new Parameter[]{s_operationCommand, s_propertiesFile, s_exportFile, s_exportMetricNames,
								s_exportRecoveryFile, s_appendToExportFile}, null));

		cl.parse(args);

		//This sends jersey java util logging to logback
		SLF4JBridgeHandler.removeHandlersForRootLogger();
		SLF4JBridgeHandler.install();

		String operation = s_operationCommand.getValue();

		if (!operation.equals("run"))
		{
			//Turn off console logging
			Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
			root.getAppender("stdout").addFilter(new Filter<ILoggingEvent>()
			{
				@Override
				public FilterReply decide(ILoggingEvent iLoggingEvent)
				{
					return (FilterReply.DENY);
				}
			});
		}

		File propertiesFile = null;
		if (s_propertiesFile.isSet())
			propertiesFile = s_propertiesFile.getValue();

		final Main main = new Main(propertiesFile);

		if (operation.equals("export"))
		{
			if (s_exportFile.isSet())
			{
				Writer ps = new OutputStreamWriter(new FileOutputStream(s_exportFile.getValue(),
						s_appendToExportFile.isSet()), "UTF-8");
				main.runExport(ps, s_exportMetricNames.getValues());
				ps.flush();
				ps.close();
			}
			else
			{
				main.runExport(new OutputStreamWriter(System.out, "UTF-8"), s_exportMetricNames.getValues());
				System.out.flush();
			}

			main.stopServices();
		}
		else if (operation.equals("import"))
		{
			if (s_exportFile.isSet())
			{
				FileInputStream fin = new FileInputStream(s_exportFile.getValue());
				main.runImport(fin);
				fin.close();
			}
			else
			{
				main.runImport(System.in);
			}

			main.stopServices();
		}
		else if (operation.equals("run") || operation.equals("start"))
		{
			try
			{
				main.startServices();

				logger.info("------------------------------------------");
				logger.info("     KairosDB service started");
				logger.info("------------------------------------------");

				Runtime.getRuntime().addShutdownHook(new Thread(new Runnable()
				{
					public void run()
					{
						try
						{
							main.stopServices();

							synchronized (s_shutdownObject)
							{
								s_shutdownObject.notify();
							}
						}
						catch (Exception e)
						{
							logger.error("Shutdown exception:", e);
						}
					}
				}));

				waitForShutdown();
			}
			catch (Exception e)
			{
				logger.error("Failed starting up services", e);
				main.stopServices();
				System.exit(1);
			}
			finally
			{
				logger.info("--------------------------------------");
				logger.info("     KairosDB service is now down!");
				logger.info("--------------------------------------");
			}
		}
	}

	public Injector getInjector()
	{
		return (m_injector);
	}

	public void runExport(Writer out, List<String> metricNames) throws DatastoreException, IOException
	{
		RecoveryFile recoveryFile = new RecoveryFile();
		try
		{
			KairosDatastore ds = m_injector.getInstance(KairosDatastore.class);
			Iterable<String> metrics;

			if (metricNames != null && metricNames.size() > 0)
				metrics = metricNames;
			else
				metrics = ds.getMetricNames();

			for (String metric : metrics)
			{
				if (!recoveryFile.contains(metric))
				{
					logger.info("Exporting: " + metric);
					QueryMetric qm = new QueryMetric(1L, 0, metric);
					ExportQueryCallback callback = new ExportQueryCallback(metric, out);
					ds.export(qm, callback);

					recoveryFile.writeMetric(metric);
				}
				else
					logger.info("Skipping metric " + metric + " because it was already exported.");
			}
		}
		finally
		{
			recoveryFile.close();
		}
	}

	public void runImport(InputStream in) throws IOException, DatastoreException
	{
		KairosDatastore ds = m_injector.getInstance(KairosDatastore.class);

		BufferedReader reader = new BufferedReader(new InputStreamReader(in, UTF_8));

		Gson gson = new Gson();
		String line;
		while ((line = reader.readLine()) != null)
		{
			JsonMetricParser jsonMetricParser = new JsonMetricParser(ds, new StringReader(line), gson);

			ValidationErrors validationErrors = jsonMetricParser.parse();

			for (String error : validationErrors.getErrors())
			{
				logger.error(error);
				System.err.println(error);
			}
		}
	}

	/**
	 * Simple technique to prevent the main thread from existing until we are done
	 */
	private static void waitForShutdown()
	{
		try
		{
			synchronized (s_shutdownObject)
			{
				s_shutdownObject.wait();
			}
		}
		catch (InterruptedException ignore)
		{
		}
	}


	public void startServices() throws KairosDBException
	{
		Map<Key<?>, Binding<?>> bindings =
				m_injector.getAllBindings();

		for (Key<?> key : bindings.keySet())
		{
			Class bindingClass = key.getTypeLiteral().getRawType();
			if (KairosDBService.class.isAssignableFrom(bindingClass))
			{
				KairosDBService service = (KairosDBService) m_injector.getInstance(bindingClass);
				m_services.add(service);

				logger.info("Starting service " + bindingClass);
				service.start();
			}
		}
	}


	public void stopServices() throws DatastoreException, InterruptedException
	{
		logger.info("Shutting down");
		for (KairosDBService service : m_services)
		{
			logger.info("Stopping " + service.getClass().getName());
			service.stop();
		}

		//Stop the datastore
		KairosDatastore ds = m_injector.getInstance(KairosDatastore.class);
		ds.close();
	}

	private class RecoveryFile
	{
		private final Set<String> metricsExported = new HashSet<String>();

		private File recoveryFile;
		private PrintWriter writer;

		public RecoveryFile() throws IOException
		{
			if (s_exportRecoveryFile.isSet())
			{
				recoveryFile = new File(s_exportRecoveryFile.getValue());
				logger.info("Tracking exported metric names in " + recoveryFile.getAbsolutePath());

				if (recoveryFile.exists())
				{
					logger.info("Skipping metrics found in " + recoveryFile.getAbsolutePath());
					List<String> list = FileUtils.readLines(recoveryFile);
					metricsExported.addAll(list);
				}

				writer = new PrintWriter(new FileOutputStream(recoveryFile, true));
			}
		}

		public boolean contains(String metric)
		{
			return metricsExported.contains(metric);
		}

		public void writeMetric(String metric)
		{
			if (writer != null)
			{
				writer.println(metric);
				writer.flush();
			}
		}

		public void close()
		{
			if (writer != null)
				writer.close();
		}
	}

	private class ExportQueryCallback implements QueryCallback
	{
		private final Writer m_writer;
		private JSONWriter m_jsonWriter;
		private final String m_metric;

		public ExportQueryCallback(String metricName, Writer out)
		{
			m_metric = metricName;
			m_writer = out;
		}

		@Override
		public void addDataPoint(long timestamp, long value) throws IOException
		{
			try
			{
				m_jsonWriter.array().value(timestamp).value(value).endArray();
			}
			catch (JSONException e)
			{
				throw new IOException(e);
			}
		}

		@Override
		public void addDataPoint(long timestamp, double value) throws IOException
		{
			try
			{
				m_jsonWriter.array().value(timestamp).value(value).endArray();
			}
			catch (JSONException e)
			{
				throw new IOException(e);
			}
		}

		@Override
		public void startDataPointSet(Map<String, String> tags) throws IOException
		{
			if (m_jsonWriter != null)
				endDataPoints();

			try
			{
				m_jsonWriter = new JSONWriter(m_writer);
				m_jsonWriter.object();
				m_jsonWriter.key("name").value(m_metric);
				m_jsonWriter.key("tags").value(tags);

				m_jsonWriter.key("datapoints").array();

			}
			catch (JSONException e)
			{
				throw new IOException(e);
			}
		}

		@Override
		public void endDataPoints() throws IOException
		{
			try
			{
				if (m_jsonWriter != null)
				{
					m_jsonWriter.endArray().endObject();
					m_writer.write("\n");
					m_jsonWriter = null;
				}
			}
			catch (JSONException e)
			{
				throw new IOException(e);
			}

		}
	}
}
