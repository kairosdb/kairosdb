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
package net.opentsdb.core;

import com.google.common.collect.SetMultimap;
import com.google.inject.*;
import jcmdline.*;
import net.opentsdb.core.datastore.DataPointGroup;
import net.opentsdb.core.datastore.Datastore;
import net.opentsdb.core.datastore.QueryMetric;
import net.opentsdb.core.exception.DatastoreException;
import net.opentsdb.core.exception.TsdbException;
import net.opentsdb.core.http.WebServletModule;
import net.opentsdb.core.telnet.TelnetServer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.ajax.JSONObjectConvertor;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

public class Main
{
	public static final Logger logger = LoggerFactory.getLogger(Main.class);

	public static final String SERVICE_PREFIX = "opentsdb.service";

	private static FileParam s_propertiesFile = new FileParam("p",
			"a custom properties file", FileParam.IS_FILE & FileParam.IS_READABLE,
			FileParam.OPTIONAL);

	private static FileParam s_exportFile = new FileParam("f",
			"file to save export to or read from depending on command", FileParam.IS_FILE,
			FileParam.OPTIONAL);

	private static StringParam s_operationCommand = new StringParam("c",
			"command to run", new String[] {"run", "export", "import"});

	private static Object s_shutdownObject = new Object();

	private Injector m_injector;
	private List<OpenTsdbService> m_services = new ArrayList<OpenTsdbService>();


	public Main(File propertiesFile) throws IOException
	{
		Properties props = new Properties();
		props.load(getClass().getClassLoader().getResourceAsStream("opentsdb.properties"));

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
							mod = (Module)constructor.newInstance(props);
						else
							mod = (Module)aClass.newInstance();

						moduleList.add(mod);
					}
				}
				catch (Exception e)
				{
					logger.error("Unable to load service "+propName, e);
				}
			}
		}

		m_injector = Guice.createInjector(moduleList);
	}


	public static void main(String[] args) throws Exception
	{
		CmdLineHandler cl = new VersionCmdLineHandler("Version 2.0",
				new HelpCmdLineHandler("Opentsdb Help", "opentsdb", "Starts OpenTSDB",
						new Parameter[] { s_operationCommand, s_propertiesFile, s_exportFile }, null));

		cl.parse(args);

		File propertiesFile = null;
		if (s_propertiesFile.isSet())
			propertiesFile = s_propertiesFile.getValue();

		final Main main = new Main(propertiesFile);

		String operation = s_operationCommand.getValue();

		if (operation.equals("export"))
		{
			if (s_exportFile.isSet())
			{
				PrintStream ps = new PrintStream(new FileOutputStream(s_exportFile.getValue()));
				main.export(ps);
				ps.close();
			}
			else
			{
				main.export(System.out);
			}
		}
		else if (operation.equals("run"))
		{
			main.startServices();

			Runtime.getRuntime().addShutdownHook(new Thread(new Runnable()
			{
				public void run()
				{
					main.stopServices();
					synchronized (s_shutdownObject)
					{
						s_shutdownObject.notify();
					}
				}
			}));

			waitForShutdown();
		}
	}

	public void export(PrintStream out) throws DatastoreException, IOException
	{
		Datastore ds = m_injector.getInstance(Datastore.class);

		Iterable<String> metrics = ds.getMetricNames();

		try
		{
			for (String metric : metrics)
			{
				QueryMetric qm = new QueryMetric(1L, 0, metric, "none");
				List<DataPointGroup> results = ds.query(qm);

				for (DataPointGroup result : results)
				{
					JSONObject tags = new JSONObject();
					SetMultimap<String, String> resTags = result.getTags();
					for (String key : resTags.keySet())
					{
						tags.put(key, resTags.get(key).iterator().next());
					}

					while (result.hasNext())
					{
						DataPoint dp = result.next();

						JSONObject jsObj = new JSONObject();
						jsObj.put("name", metric);
						jsObj.put("time", dp.getTimestamp());

						if (dp.isInteger())
							jsObj.put("value", dp.getLongValue());
						else
							jsObj.put("value", dp.getDoubleValue());

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

	/**
	 Simple technique to prevent the main thread from existing until we are done
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


	public void startServices() throws TsdbException
	{
		Map<Key<?>, Binding<?>> bindings =
				m_injector.getAllBindings();

		for (Key<?> key : bindings.keySet())
		{
			Class bindingClass = key.getTypeLiteral().getRawType();
			Set<Class> interfaces = new HashSet<Class>(Arrays.asList(bindingClass.getInterfaces()));
			if (interfaces.contains(OpenTsdbService.class))
			{
				OpenTsdbService service = (OpenTsdbService)m_injector.getInstance(bindingClass);
				m_services.add(service);

				logger.info("Starting service "+bindingClass);
				service.start();
			}
		}
	}


	public void stopServices()
	{
		System.err.println("Shutting down");
		for (OpenTsdbService service : m_services)
		{
			service.stop();
		}
	}
}
