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

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.inject.Binding;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import org.h2.util.StringUtils;
import org.json.JSONException;
import org.json.JSONWriter;
import org.kairosdb.core.datastore.DatastoreQuery;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.datastore.QueryCallback;
import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.core.http.rest.json.DataPointsParser;
import org.kairosdb.core.http.rest.json.ValidationErrors;
import org.kairosdb.eventbus.FilterEventBus;
import org.kairosdb.eventbus.Publisher;
import org.kairosdb.events.DataPointEvent;
import org.kairosdb.events.ShutdownEvent;
import org.kairosdb.util.PluginClassLoader;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.Writer;
import java.lang.reflect.Constructor;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Main
{
	public static final Logger logger = (Logger) LoggerFactory.getLogger(Main.class);

	public static final Charset UTF_8 = Charset.forName("UTF-8");
	public static final String SERVICE_PREFIX = "kairosdb.service.";
	public static final String SERVICE_FOLDER_PREFIX = "kairosdb.service_folder.";

	private final static CountDownLatch s_shutdownObject = new CountDownLatch(1);

	private static final Arguments arguments = new Arguments();

	private Injector m_injector;
	private List<KairosDBService> m_services = new ArrayList<KairosDBService>();

	private void loadPlugins(KairosConfig config, final File propertiesFile) throws IOException
	{
		File propDir = propertiesFile.getParentFile();
		if (propDir == null)
			propDir = new File(".");

		String[] pluginProps = propDir.list(new FilenameFilter()
		{
			@Override
			public boolean accept(File dir, String name)
			{
				try
				{
					KairosConfig.ConfigFormat format = KairosConfig.ConfigFormat.fromFileName(name);
					return (config.isSupportedFormat(format)) && !name.equals(propertiesFile.getName());
				}
				catch (IllegalArgumentException ignored) {}
				return false;
			}
		});
		if (pluginProps == null)
			return;
		
		ClassLoader cl = getClass().getClassLoader();

		for (String prop : pluginProps)
		{
			logger.info("Loading plugin properties: {}", prop);
			//Load the properties file from a jar if there is one first.
			//This way defaults can be set
			InputStream propStream = cl.getResourceAsStream(prop);

			if (propStream != null)
			{
				try
				{
					config.load(propStream, KairosConfig.ConfigFormat.fromFileName(prop));
				}
				finally
				{
					propStream.close();
				}
			}

			//Load the file in
			config.load(new File(propDir, prop));
		}
	}

	private URL[] getJarsInPath(String path) throws MalformedURLException
	{
		List<URL> jars = new ArrayList<URL>();
		File libDir = new File(path);
		File[] fileList = libDir.listFiles();
		if(fileList != null)
		{
			for (File f : fileList)
			{
				if (f.getName().endsWith(".jar"))
				{
					jars.add(f.toURI().toURL());
				}
			}
		}

		System.out.println(jars);
		return jars.toArray(new URL[0]);
	}

	protected static String toEnvVarName(String propName) {
		return propName.toUpperCase().replace('.', '_');
	}

	/*
	 * allow overwriting any existing property via correctly named environment variable
	 * e.g. kairosdb.datastore.cassandra.host_list via KAIROSDB_DATASTORE_CASSANDRA_HOST_LIST
	 */
	protected void applyEnvironmentVariables(KairosConfig config)
	{
		Map<String, String> env = System.getenv();
		Map<String, String> props = new HashMap<>();
		for (String propName : config)
		{
			String envVarName = toEnvVarName(propName);
			if (env.containsKey(envVarName))
			{
				props.put(propName, env.get(envVarName));
			}
		}

		config.load(props);
	}

	public Main(File propertiesFile) throws IOException
	{
		KairosConfig config = new KairosConfig();
		String defaultConfig = "kairosdb.conf";
		try (InputStream is = getClass().getClassLoader().getResourceAsStream(defaultConfig))
		{
			config.load(is, KairosConfig.ConfigFormat.fromFileName(defaultConfig));
		}

		if (propertiesFile != null)
		{
			config.load(propertiesFile);
			loadPlugins(config, propertiesFile);
		}

		config.loadSystemProperties();

		applyEnvironmentVariables(config);

		config.resolve();

		/*for (String s : config)
		{
			System.out.println(s);
		}*/

		List<Module> moduleList = new ArrayList<Module>();
		moduleList.add(new CoreModule(config));

		for (String propName : config)
		{
			if (propName.startsWith(SERVICE_PREFIX))
			{
				Class<?> aClass;
				try
				{
					if ("".equals(config.getProperty(propName)) || "<disabled>".equals(config.getProperty(propName)))
						continue;

					String serviceName = propName.substring(SERVICE_PREFIX.length());

					String pluginFolder = config.getProperty(SERVICE_FOLDER_PREFIX + serviceName);

					ClassLoader pluginLoader = this.getClass().getClassLoader();

					/*
					Check to see if a folder for the plugin exists.  If it does we
					create a new class loader for the plugin's jars, this isolates
					dependencies.
					 */
					if (pluginFolder != null)
					{
						pluginLoader = new PluginClassLoader(getJarsInPath(pluginFolder), pluginLoader);
					}

					aClass = pluginLoader.loadClass(config.getProperty(propName));
					if (Module.class.isAssignableFrom(aClass))
					{
						Constructor<?> constructor = null;

						try
						{
							constructor = aClass.getConstructor(KairosConfig.class);
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
							mod = (Module) constructor.newInstance(config);
						else
							mod = (Module) aClass.newInstance();

						if (mod instanceof CoreModule)
						{
							mod = Modules.override(moduleList.get(0)).with(mod);
							moduleList.set(0, mod);
						}
						else
							moduleList.add(mod);
					}
				}
				catch (Exception e)
				{
					logger.error("Unable to load service " + propName, e);
				}
			}
		}

		System.out.println("here");
		m_injector = Guice.createInjector(moduleList);
		System.out.println("done");
	}


	public static void main(String[] args) throws Exception
	{
		//This sends jersey java util logging to logback
		SLF4JBridgeHandler.removeHandlersForRootLogger();
		SLF4JBridgeHandler.install();

		JCommander commander = new JCommander(arguments);
		try
		{
			commander.parse(args);
		}
		catch (Exception e)
		{
			System.out.println(e.getMessage());
			commander.usage();
			System.exit(0);
		}

		if (arguments.helpMessage || arguments.help)
		{
			commander.usage();
			System.exit(0);
		}

		if (!arguments.operationCommand.equals("run"))
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
		if (!StringUtils.isNullOrEmpty(arguments.propertiesFile))
			propertiesFile = new File(arguments.propertiesFile);

		try
		{
			final Main main = new Main(propertiesFile);

			if (arguments.operationCommand.equals("export"))
			{
				if (!StringUtils.isNullOrEmpty(arguments.exportFile))
				{
					Writer ps = new OutputStreamWriter(new FileOutputStream(arguments.exportFile,
							arguments.appendToExportFile), "UTF-8");
					main.runExport(ps, arguments.exportMetricNames);
					ps.flush();
					ps.close();
					System.out.println("Export finished");
				}
				else
				{
					OutputStreamWriter writer = new OutputStreamWriter(System.out, "UTF-8");
					main.runExport(writer, arguments.exportMetricNames);
					writer.flush();
				}

				main.stopServices();
				System.out.println("All done");
			}
			else if (arguments.operationCommand.equals("import"))
			{
				if (!StringUtils.isNullOrEmpty(arguments.exportFile))
				{
					FileInputStream fin = new FileInputStream(arguments.exportFile);
					main.runImport(fin);
					fin.close();
				}
				else
				{
					main.runImport(System.in);
				}
				System.out.println("Import finished");
				Thread.sleep(10000);

				main.stopServices();
				System.out.println("All done");
			}
			else if (arguments.operationCommand.equals("run") || arguments.operationCommand.equals("start"))
			{
				try
				{
					Runtime.getRuntime().addShutdownHook(new Thread(new Runnable()
					{
						public void run()
						{
							try
							{
								main.stopServices();

								s_shutdownObject.countDown();
							}
							catch (Exception e)
							{
								logger.error("Shutdown exception:", e);
							}
						}
					}));

					main.startServices();

					logger.info("------------------------------------------");
					logger.info("     KairosDB service started");
					logger.info("------------------------------------------");

					//main.runMissTest();
					waitForShutdown();
				}
				catch (Exception e)
				{
					logger.error("Failed starting up services", e);
					//main.stopServices();
					System.exit(0);
				}
				finally
				{
					logger.info("--------------------------------------");
					logger.info("     KairosDB service is now down!");
					logger.info("--------------------------------------");
				}

			}
		}
		catch (Exception e)
		{
			logger.error("Failed to startup", e);
		}
	}

	public Injector getInjector()
	{
		return (m_injector);
	}

	public void runMissTest()
	{
		try
		{
			KairosDatastore ds = m_injector.getInstance(KairosDatastore.class);

			long start = System.currentTimeMillis();
			int I;

			for (I = 0; I < 100000; I++)
			{
				String metricName = UUID.randomUUID().toString();
				DatastoreQuery query = ds.createQuery(new QueryMetric(0, 0, "abc123" + metricName));
				query.execute();
				query.close();
			}

			long stop = System.currentTimeMillis();
			long time = stop - start;
			System.out.println(time);
			System.out.println((I * 1000) / time);

		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
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
		FilterEventBus eventBus = m_injector.getInstance(FilterEventBus.class);
		Publisher<DataPointEvent> publisher = eventBus.createPublisher(DataPointEvent.class);
		KairosDataPointFactory dpFactory = m_injector.getInstance(KairosDataPointFactory.class);

		BufferedReader reader = new BufferedReader(new InputStreamReader(in, UTF_8));

		Gson gson = new Gson();
		String line;
		while ((line = reader.readLine()) != null)
		{
			DataPointsParser dataPointsParser = new DataPointsParser(publisher, new StringReader(line),
					gson, dpFactory);

			ValidationErrors validationErrors = dataPointsParser.parse();

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
			s_shutdownObject.await();
		}
		catch (InterruptedException ignore)
		{
			Thread.currentThread().interrupt();
		}
	}


	public void startServices() throws KairosDBException
	{
		Map<Key<?>, Binding<?>> bindings =
				m_injector.getAllBindings();

		for (Key<?> key : bindings.keySet())
		{
			Class<?> bindingClass = key.getTypeLiteral().getRawType();
			if (KairosDBService.class.isAssignableFrom(bindingClass))
			{
				KairosDBService service = (KairosDBService) m_injector.getInstance(bindingClass);
				logger.info("Starting service " + bindingClass);
				service.start();
				m_services.add(service);
			}
		}
	}


	public void stopServices() throws DatastoreException, InterruptedException
	{
		logger.info("Shutting down");
		for (KairosDBService service : m_services)
		{
			String serviceName = service.getClass().getName();
			logger.info("Stopping " + serviceName);
			try
			{
				service.stop();
				logger.info("Stopped  " + serviceName);
			}
			catch (Exception e)
			{
				logger.error("Error stopping " + serviceName, e);
			}
		}

		logger.info("Stopping Datastore");
		//Stop the datastore
		KairosDatastore ds = m_injector.getInstance(KairosDatastore.class);
		ds.close();

		FilterEventBus eventBus = m_injector.getInstance(FilterEventBus.class);
		eventBus.createPublisher(ShutdownEvent.class).post(new ShutdownEvent());
	}

	private static class RecoveryFile
	{
		private final Set<String> metricsExported = new HashSet<String>();

		private File recoveryFile;
		private PrintWriter writer;

		public RecoveryFile() throws IOException
		{
			if (!StringUtils.isNullOrEmpty(arguments.exportRecoveryFile))
			{
				recoveryFile = new File(arguments.exportRecoveryFile);
				logger.info("Tracking exported metric names in " + recoveryFile.getAbsolutePath());

				if (recoveryFile.exists())
				{
					logger.info("Skipping metrics found in " + recoveryFile.getAbsolutePath());
					List<String> list = Files.readLines(recoveryFile, Charset.defaultCharset());
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

	private static class ExportQueryCallback implements QueryCallback
	{
		private final Writer m_writer;
		private JSONWriter m_jsonWriter;
		private final String m_metric;
		private final ReentrantReadWriteLock m_lock = new ReentrantReadWriteLock();

		public ExportQueryCallback(String metricName, Writer out)
		{
			m_metric = metricName;
			m_writer = out;
		}

		@Override
		public DataPointWriter startDataPointSet(String type, SortedMap<String, String> tags) throws IOException
		{
			m_lock.writeLock().lock();

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

			return new ExportDataPointWriter();
		}

		private class ExportDataPointWriter implements DataPointWriter
		{

			@Override
			public void addDataPoint(DataPoint datapoint) throws IOException
			{
				try
				{
					m_jsonWriter.array().value(datapoint.getTimestamp());
					datapoint.writeValueToJson(m_jsonWriter);
					m_jsonWriter.value(datapoint.getApiDataType()).endArray();
				}
				catch (JSONException e)
				{
					throw new IOException(e);
				}
			}

			@Override
			public void close() throws IOException
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
				finally
				{
					m_lock.writeLock().unlock();
				}
			}
		}
	}

	@SuppressWarnings("UnusedDeclaration")
	private static class Arguments
	{
		@Parameter(names = "-p", description = "A custom properties file")
		private String propertiesFile;

		@Parameter(names = "-f", description = "File to save export to or read from depending on command.")
		private String exportFile;

		@Parameter(names = "-n", description = "Name of metrics to export. If not specified, then all metrics are exported.")
		private List<String> exportMetricNames;

		@Parameter(names = "-r", description = "Full path to a recovery file. The file tracks metrics that have been exported. " +
				"If export fails and is run again it uses this file to pickup where it left off.")
		private String exportRecoveryFile;

		@Parameter(names = "-a", description = "Appends to the export file. By default, the export file is overwritten.")
		private boolean appendToExportFile;

		@Parameter(names = "--help", description = "Help message.", help = true)
		private boolean helpMessage;

		@Parameter(names = "-h", description = "Help message.", help = true)
		private boolean help;

		/**
		 * start is identical to run except that logging data only goes to the log file
		 * and not to standard out as well
		 */
		@Parameter(names = "-c", description = "Command to run: export, import, run, start.")
		private String operationCommand;
	}
}
