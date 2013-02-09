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

import com.google.inject.*;
import jcmdline.*;
import net.opentsdb.core.exception.TsdbException;
import net.opentsdb.core.http.WebServletModule;
import net.opentsdb.core.telnet.TelnetServer;
import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
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

	private static Object s_shutdownObject = new Object();

	private Injector m_injector;
	private List<OpenTsdbService> m_services = new ArrayList<OpenTsdbService>();


	public Main() throws IOException
	{
		Properties props = new Properties();
		props.load(getClass().getClassLoader().getResourceAsStream("opentsdb.properties"));

		if (s_propertiesFile.isSet())
			props.load(new FileInputStream(s_propertiesFile.getValue()));

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
						System.out.println(aClass.getName());
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
						new Parameter[] { s_propertiesFile }, null));

		cl.parse(args);
		final Main main = new Main();

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
