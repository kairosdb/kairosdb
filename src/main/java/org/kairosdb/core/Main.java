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
import com.google.inject.*;
import jcmdline.*;
import org.json.JSONException;
import org.json.JSONObject;
import org.kairosdb.core.datastore.DataPointRow;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.exception.KariosDBException;
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

	/**
	start is identical to run except that logging data only goes to the log file
	and not to standard out as well
	*/
	private static StringParam s_operationCommand = new StringParam("c",
			"command to run", new String[]{"run", "start", "export", "import"});

	private static Object s_shutdownObject = new Object();

	private Injector m_injector;
	private List<KairosDBService> m_services = new ArrayList<KairosDBService>();


	public Main(File propertiesFile) throws IOException
	{
		Properties props = new Properties();
		props.load(getClass().getClassLoader().getResourceAsStream("kairosdb.properties"));

		if (propertiesFile != null)
			props.load(new FileInputStream(propertiesFile));

		List<Module> moduleList = new ArrayList<Module>();
		moduleList.add(new CoreModule(props));

		for (String propName : props.stringPropertyNames())
		{
			if (propName.startsWith(SERVICE_PREFIX))
			{
				Class<?> aClass = null;
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
						catch (NoSuchMethodException nsme)
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
						new Parameter[]{s_operationCommand, s_propertiesFile, s_exportFile}, null));

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
				PrintStream ps = new PrintStream(new FileOutputStream(s_exportFile.getValue()));
				main.runExport(ps);
				ps.close();
			}
			else
			{
				main.runExport(System.out);
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

	public void runExport(PrintStream out) throws DatastoreException, IOException
	{
		KairosDatastore ds = m_injector.getInstance(KairosDatastore.class);

		Iterable<String> metrics = ds.getMetricNames();

		//metrics = Arrays.asList("cpu.temperature");

		try
		{
			for (String metric : metrics)
			{
				logger.info("Exporting: " + metric);
				QueryMetric qm = new QueryMetric(1L, 0, metric);
				List<DataPointRow> results = ds.export(qm);

				for (DataPointRow result : results)
				{
					JSONObject tags = new JSONObject();
					for (String key : result.getTagNames())
					{
						tags.put(key, result.getTagValue(key));
					}

					while (result.hasNext())
					{
						DataPoint dp = result.next();

						JSONObject jsObj = new JSONObject();
						jsObj.put("name", metric);
						jsObj.put("time", dp.getTimestamp());

						if (dp.isInteger())
						{
							jsObj.put("int_value", true);
							jsObj.put("value", dp.getLongValue());
						}
						else
						{
							jsObj.put("int_value", false);
							jsObj.put("value", dp.getDoubleValue());
						}

						jsObj.put("tags", tags);

						out.println(jsObj.toString());
					}
				}
			}
		}
		catch (JSONException e)
		{
			e.printStackTrace();
		}

		out.flush();
	}

	public void runImport(InputStream in) throws IOException, JSONException, DatastoreException
	{
		KairosDatastore ds = m_injector.getInstance(KairosDatastore.class);

		BufferedReader reader = new BufferedReader(new InputStreamReader(in, UTF_8));

		String line = null;
		while ((line = reader.readLine()) != null)
		{
			JSONObject metric = new JSONObject(line);

			DataPointSet dps = new DataPointSet(metric.getString("name"));

			JSONObject tags = metric.getJSONObject("tags");
			Iterator<Object> keys = tags.keys();
			while (keys.hasNext())
			{
				String tagName = (String) keys.next();
				String tagValue = tags.getString(tagName);

				dps.addTag(tagName, tagValue);
			}

			DataPoint dp;
			if (metric.getBoolean("int_value"))
				dp = new DataPoint(metric.getLong("time"), metric.getLong("value"));
			else
				dp = new DataPoint(metric.getLong("time"), metric.getDouble("value"));

			dps.addDataPoint(dp);

			ds.putDataPoints(dps);
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
		catch (InterruptedException e)
		{
		}
	}


	public void startServices() throws KariosDBException
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
			logger.info("Stopping "+service.getClass().getName());
			service.stop();
		}

		//Stop the datastore
		KairosDatastore ds = m_injector.getInstance(KairosDatastore.class);
		ds.close();
	}
}
